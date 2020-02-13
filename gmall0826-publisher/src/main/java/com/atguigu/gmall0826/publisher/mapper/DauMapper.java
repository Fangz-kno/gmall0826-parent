package com.atguigu.gmall0826.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
//    通过参数日期查询phoenix 得到long型结果
    public Long selectDauTotal(String date);

    public List<Map> selectDauHourCount(String date);

}
