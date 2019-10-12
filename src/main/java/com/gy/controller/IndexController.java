package com.gy.controller;


import com.gy.index.Sku;
import com.gy.utils.IndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * created by yangyu on 2019-09-17
 */
@RestController
@RequestMapping(value = "/index")
public class IndexController {

    private static final Logger logger = LoggerFactory.getLogger(IndexController.class);
    private static final String INDEX = "mysqltoessku";

    @Autowired
    private IndexUtils indexUtils;

    @PostMapping(value = "/create", produces = "application/json;charset=UTF-8")
    @ResponseBody
    public boolean create(){
        boolean isSuccess = false;
        try {
            isSuccess = indexUtils.createIndexWithJavaClass(Sku.class);
        }catch (Exception ex){
            ex.printStackTrace();
            logger.error("Create Index Failed : ",ex);
        }
        return isSuccess;

    }

}
