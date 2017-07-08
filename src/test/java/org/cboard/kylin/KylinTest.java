package org.cboard.kylin;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.support.BasicAuthorizationInterceptor;
import org.springframework.web.client.RestTemplate;

/**
 * Created by yunbo on 2017/07/08 0008.
 */
public class KylinTest {

    @Test
    public void test_get_model_with_wrong_name() throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        String username = "ADMIN";
        String password = "KYLIN";
        restTemplate.getInterceptors().add(new BasicAuthorizationInterceptor(username, password));
        String serverIp = "192.168.0.231:7070";
        ResponseEntity<String> a = restTemplate.getForEntity("http://" + serverIp + "/kylin/api/model/{modelName}", String.class, "KeHuShuLiangHong");
        JSONObject jsonObject = JSONObject.parseObject(a.getBody());
        System.out.println("jsonObject : " + jsonObject.toString());
        KylinModel model = new KylinModel(jsonObject, serverIp, username, password);
        System.out.println("columns count : " + model.getColumns().length);
    }
}
