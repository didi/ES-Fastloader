package com.didichuxing.fastindex.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;

@Slf4j
public class HttpUtil {

    public static enum HttpType {
        GET("get"), PUT("put"), POST("post"), DELETE("delete");

        private String type;
        HttpType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    public static String doHttp(String url, JSONObject param, String body, HttpType httpType) {
        String logInfo = "url:" + url + ", param:" + param + ", body:" + body + ", type:" + httpType.getType();

        if (param != null) {
            StringBuilder sb = new StringBuilder();
            for (String key : param.keySet()) {
                sb.append(key).append("=").append(param.getString(key)).append("&");
            }

            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }

            url = url + "?" + sb.toString();
        }

        String result = "";
        try {
            switch (httpType) {
                case GET:
                    return HttpUtil.get(url);
                case PUT:
                    return HttpUtil.putJsonEntity(url, body);
                case POST:
                    return HttpUtil.postJsonEntity(url, body);
            }

            return null;
        } catch (Throwable t) {
            return null;
        }
    }


    public static String putJsonEntity(String url, String body) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpPut httpPut= null;
        try {
            httpPut = new HttpPut(url);
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5 * 60 * 1000);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 5 * 60 * 1000);
            if(body!=null) {
                httpPut.setEntity(new StringEntity(body, "utf-8"));
            }
            httpPut.setHeader("Content-Type", "application/json");
            HttpResponse response = httpClient.execute(httpPut);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String result = EntityUtils.toString(entity, "utf-8");
                return result;
            }
        } catch (Throwable t) {
            log.error("http put json entity error, url:" + url + ", msg:" + t.getMessage(), t);
        } finally {
            try {
                if (httpPut!= null) {
                    httpPut.releaseConnection();
                }
            } catch (Throwable t) {
                log.error("http put json entity relase connection error, url:" + url + ", msg:" + t.getMessage(), t);
            }
        }
        return null;
    }

    public static String postJsonEntity(String url, String body) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpPost httpPost = null;
        try {
            httpPost = new HttpPost(url);
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5 * 60 * 1000);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 10 * 60 * 1000);
            if(body!=null) {
                httpPost.setEntity(new StringEntity(body, "utf-8"));
            }
            httpPost.setHeader("Content-Type", "application/json");
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String result = EntityUtils.toString(entity, "utf-8");
                return result;
            }
        } catch (Throwable t) {
            log.error("http post json entity error, url:" + url + ", msg:" + t.getMessage(), t);
        } finally {
            try {
                if (httpPost != null) {
                    httpPost.releaseConnection();
                }
            } catch (Throwable t) {
                log.error("http post json entity relase connection error, url:" + url + ", msg:" + t.getMessage(), t);
            }
        }
        return null;
    }

    public static String get(String url) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpGet httpGet = null;
        try {
            httpGet = new HttpGet(url);
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5 * 60 * 1000);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 5 * 60 * 1000);
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String result = EntityUtils.toString(entity, "UTF-8");
                return result;
            }
        } catch (Throwable t) {
            log.error("http get error, url:" + url + ", msg:" + t.getMessage(), t);
        } finally {
            try {
                if (httpGet != null) {
                    httpGet.releaseConnection();
                }
            } catch (Throwable t) {
                log.error("http get release connection error, url:" + url + ", msg:" + t.getMessage(), t);
            }
        }
        return null;
    }

}
