package com.gy.config;

import java.io.IOException;

/**
 * created by yangyu on 2019-09-17
 */
public class ESIoException extends Exception{

    public ESIoException(String msg, IOException e){
        super(msg,e);
    }
}
