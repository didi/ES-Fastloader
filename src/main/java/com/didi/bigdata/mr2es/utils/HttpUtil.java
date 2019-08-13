package com.didi.bigdata.mr2es.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;

/**
 * 由于hadoop 平台使用的httpclient版本较低, 所以此处使用4.2.5版本, 而非4.5.2版本
 */
public class HttpUtil {

    public static void main(String[] args) {
    }


    public static String postJsonEntity(String url, JSONObject json) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpPost httpPost = null;
        try {
            httpPost = new HttpPost(url);
            httpPost.setEntity(new StringEntity(json.toJSONString(), "utf-8"));
            httpPost.setHeader("Content-Type", "application/json");
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String result = EntityUtils.toString(entity, "utf-8");
                return result;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpPost != null) {
                    httpPost.releaseConnection();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "";
    }


    public static String postEsByDsl(String url, String dsl, String token) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpPost httpPost = null;
        try {
            httpPost = new HttpPost(url);
            httpPost.setEntity(new StringEntity(dsl, "utf-8"));
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setHeader("Authorization", token);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String result = EntityUtils.toString(entity, "utf-8");
                return result;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpPost != null) {
                    httpPost.releaseConnection();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "";
    }


    public static String post(String url) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpPost httpPost = null;
        try {
            httpPost = new HttpPost(url);
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,
                    2000);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,
                    2000);
            httpPost.setHeader("Content-Type", "text/plain;charset=utf-8");
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String result = EntityUtils.toString(entity, "utf-8");
                return result;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpPost != null) {
                    httpPost.releaseConnection();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "";
    }

    public static String get(String url) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpGet httpGet = null;
        try {
            httpGet = new HttpGet(url);
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,
                    5 * 60 * 1000);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,
                    5 * 60 * 1000);
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String result = EntityUtils.toString(entity, "UTF-8");
                return result;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpGet != null) {
                    httpGet.releaseConnection();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "";
    }

}
