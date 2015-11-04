package com.epam.preprod.model;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLog {
    private String ip;
    private Integer requestBytes;
    private String browser;


    public AccessLog() {
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getRequestBytes() {
        return requestBytes;
    }

    public void setRequestBytes(Integer requestBytes) {
        this.requestBytes = requestBytes;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    @Override
    public String toString() {
        return String.format("[%s, %d]", ip, requestBytes);
    }
}
