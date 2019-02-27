package com.myFlink.java.cep.login;

import java.io.Serializable;

public class LoginEvent implements Serializable {

    private static final long serialVersionUID = -5472656360574184093L;

    private String userId;//用户ID
    private String ip;//登录IP
    private String type;//登录类型

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ip, String type) {
        this.userId = userId;
        this.ip = ip;
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}