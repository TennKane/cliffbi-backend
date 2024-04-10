package com.cliff.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.cliff.model.dto.chart.ChartQueryRequest;
import com.cliff.model.dto.chart.ChartRegenRequest;
import com.cliff.model.entity.Chart;
import com.baomidou.mybatisplus.extension.service.IService;
import com.cliff.model.vo.BiResponse;

import javax.servlet.http.HttpServletRequest;

/**
 *
 */
public interface ChartService extends IService<Chart> {

    QueryWrapper<Chart> getQueryWrapper(ChartQueryRequest chartQueryRequest);

    BiResponse regenChartByAsyncMq(ChartRegenRequest chartRegenRequest, HttpServletRequest request);
}
