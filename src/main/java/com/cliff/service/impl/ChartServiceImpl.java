package com.cliff.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.cliff.bizmq.BiMessageProducer;
import com.cliff.common.ErrorCode;
import com.cliff.constant.CommonConstant;
import com.cliff.constant.RedisConstant;
import com.cliff.exception.BusinessException;
import com.cliff.exception.ThrowUtils;
import com.cliff.manager.RedisLimiterManager;
import com.cliff.model.dto.chart.ChartQueryRequest;
import com.cliff.model.dto.chart.ChartRegenRequest;
import com.cliff.model.entity.Chart;
import com.cliff.model.entity.User;
import com.cliff.model.vo.BiResponse;
import com.cliff.service.ChartService;
import com.cliff.mapper.ChartMapper;
import com.cliff.service.UserService;
import com.cliff.utils.SqlUtils;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.concurrent.ExecutionException;

/**
 *
 */
@Service
@Slf4j
public class ChartServiceImpl extends ServiceImpl<ChartMapper, Chart>
    implements ChartService{


    @Resource
    private ChartMapper chartMapper;

    @Resource
    private UserService userService;

    @Resource
    private RedisLimiterManager redisLimiterManager;

    @Resource
    private BiMessageProducer biMessageProducer;


    @Resource
    private Retryer<Boolean> retryer;


    @Override
    public QueryWrapper<Chart> getQueryWrapper(ChartQueryRequest chartQueryRequest) {
        QueryWrapper<Chart> queryWrapper = new QueryWrapper<>();
        if (chartQueryRequest == null) {
            return queryWrapper;
        }

        Long id = chartQueryRequest.getId();
        String name = chartQueryRequest.getName();
        String goal = chartQueryRequest.getGoal();
        String chartType = chartQueryRequest.getChartType();
        Long userId = chartQueryRequest.getUserId();
        String sortField = chartQueryRequest.getSortField();
        String sortOrder = chartQueryRequest.getSortOrder();

        // 根据查询条件查询
        queryWrapper.eq( id != null && id > 0, "id", id);
        queryWrapper.like(StringUtils.isNotBlank(name), "name", name);
        queryWrapper.eq(StringUtils.isNotBlank(goal), "goal", goal);
        queryWrapper.eq(StringUtils.isNotBlank(goal), "goal", goal);
        queryWrapper.eq(StringUtils.isNotBlank(chartType), "chartType", chartType);
        queryWrapper.eq(ObjectUtils.isNotEmpty(userId), "userId", userId);
        queryWrapper.eq("isDelete", false);
        queryWrapper.orderBy(SqlUtils.validSortField(sortField), sortOrder.equals(CommonConstant.SORT_ORDER_ASC),
                sortField);
        return queryWrapper;
    }
    @Override
    public BiResponse regenChartByAsyncMq(ChartRegenRequest chartRegenRequest, HttpServletRequest request) {
        // 参数校验
        Long chartId = chartRegenRequest.getId();
        String name = chartRegenRequest.getName();
        String goal = chartRegenRequest.getGoal();
        String chartData = chartRegenRequest.getChartData();
        String chartType = chartRegenRequest.getChartType();
        ThrowUtils.throwIf(chartId == null || chartId <= 0, ErrorCode.PARAMS_ERROR, "图表不存在");
        ThrowUtils.throwIf(StringUtils.isBlank(name), ErrorCode.PARAMS_ERROR, "图表名称为空");
        ThrowUtils.throwIf(StringUtils.isBlank(goal), ErrorCode.PARAMS_ERROR, "分析目标为空");
        ThrowUtils.throwIf(StringUtils.isBlank(chartData), ErrorCode.PARAMS_ERROR, "原始数据为空");
        ThrowUtils.throwIf(StringUtils.isBlank(chartType), ErrorCode.PARAMS_ERROR, "图表类型为空");
        // 查看重新生成的图标是否存在
        ChartQueryRequest chartQueryRequest = new ChartQueryRequest();
        chartQueryRequest.setId(chartId);
        Long chartCount = chartMapper.selectCount(this.getQueryWrapper(chartQueryRequest));
        if (chartCount <= 0) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "图表不存在");
        }
        // 获取登录用户
        User loginUser = userService.getLoginUser(request);
        Long userId = loginUser.getId();
        // 限流
        redisLimiterManager.doRateLimit(RedisConstant.REDIS_LIMITER_ID + userId);
        // 更改图表状态为wait
        Chart waitingChart = new Chart();
        BeanUtils.copyProperties(chartRegenRequest, waitingChart);
        waitingChart.setStatus("wait");
        boolean updateResult = this.updateById(waitingChart);
        // 将修改后的图表信息保存至数据库
        if (updateResult) {
            log.info("修改后的图表信息初次保存至数据库成功");
            // 初次保存成功，则向MQ投递消息
            trySendMessageByMq(chartId);
            BiResponse biResponse = new BiResponse();
            biResponse.setChartId(chartId);
            return biResponse;
        } else {    // 保存失败则继续重试尝试保存
            try {
                Boolean callResult = retryer.call(() -> {
                    boolean retryResult = this.updateById(waitingChart);
                    if (!retryResult) {
                        log.warn("修改后的图表信息保存至数据库仍然失败，进行重试...");
                    }
                    return !retryResult;
                });
                if (callResult) {
                    trySendMessageByMq(chartId);
                }
                BiResponse biResponse = new BiResponse();
                biResponse.setChartId(chartId);
                return biResponse;
            } catch (RetryException e) {
                // 如果重试了出现异常就要将图表状态更新为failed，并打印日志
                log.error("修改后的图表信息重试保存至数据库失败", e);
                throw new BusinessException(ErrorCode.SYSTEM_ERROR, "修改后的图表信息重试保存至数据库失败");
            } catch (ExecutionException e) {
                log.error("修改后的图表信息重试保存至数据库失败", e);
                throw new BusinessException(ErrorCode.SYSTEM_ERROR, "修改后的图表信息重试保存至数据库失败");
            }
        }
    }

    public void trySendMessageByMq(long chartId) {
        try {
            biMessageProducer.sendMessage(String.valueOf(chartId));
        } catch (Exception e) {
            log.error("图表成功保存至数据库，但是消息投递失败");
            Chart failedChart = new Chart();
            failedChart.setId(chartId);
            failedChart.setStatus("failed");
            boolean b = this.updateById(failedChart);
            if (!b) {
                throw new RuntimeException("修改图表状态信息为失败失败了");
            }
            throw new BusinessException(ErrorCode.SYSTEM_ERROR, "调用 AI 接口失败");
        }
    }
}




