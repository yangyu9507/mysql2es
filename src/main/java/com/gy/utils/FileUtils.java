package com.gy.utils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * created by yangyu on 2019-09-20
 */
public class FileUtils {

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    private static ThreadLocal<DefaultResourceLoader> resourceLoaderThreadLocal =
            ThreadLocal.withInitial(DefaultResourceLoader::new);

    public static String readSqlFromFile(String filePath){
        String sql = "";
        try {
            ResourceLoader resourceLoader = resourceLoaderThreadLocal.get();
            Resource resource = resourceLoader.getResource("classpath:"+filePath);
            sql = IOUtils.toString(resource.getInputStream(), Charset.forName("UTF-8"));
        }catch (IOException ex){
            logger.error(String.format("Read File[%s] failed:",filePath),ex);
        }
        return sql;
    }
}
