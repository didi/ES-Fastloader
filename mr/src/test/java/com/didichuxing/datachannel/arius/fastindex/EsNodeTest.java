package com.didichuxing.datachannel.arius.fastindex;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.didichuxing.datachannel.arius.fastindex.es.ESClient;
import com.didichuxing.datachannel.arius.fastindex.es.ESNode;
import com.didichuxing.datachannel.arius.fastindex.es.EsWriter;
import com.didichuxing.datachannel.arius.fastindex.es.config.IndexConfig;
import com.didichuxing.datachannel.arius.fastindex.remote.RemoteService;
import com.didichuxing.datachannel.arius.fastindex.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import org.junit.Test;

import java.util.Map;

public class EsNodeTest {

    @Test
    public void ttest() throws Exception {

    }


    @Test
    public void test() throws Exception {
        TaskConfig taskConfig = new TaskConfig();
//        IndexInfo indexInfo = RemoteService.getTemplateConfig("cn_puh_ig_alarm", System.currentTimeMillis());
//        RemoteService.submitMapping("cn_puh_ig_alarm", System.currentTimeMillis(), 1, indexInfo.getSetting());


        String str = " {\n" +
                "    \"aliases\" : { },\n" +
                "    \"mappings\" : {\n" +
                "      \"_default_\" : {\n" +
                "        \"_all\" : {\n" +
                "          \"enabled\" : false\n" +
                "        },\n" +
                "        \"dynamic_date_formats\" : [\n" +
                "          \"strict_date_optional_time\",\n" +
                "          \"yyyy-MM-dd HH:mm:ss Z||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS Z||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss,SSS||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd HH:mm:ss,SSS Z||yyyy/MM/dd HH:mm:ss,SSS Z||epoch_millis\"\n" +
                "        ],\n" +
                "        \"dynamic_templates\" : [\n" +
                "          {\n" +
                "            \"extractLevel_fields\" : {\n" +
                "              \"match\" : \"extractLevel\",\n" +
                "              \"match_mapping_type\" : \"string\",\n" +
                "              \"mapping\" : {\n" +
                "                \"ignore_above\" : 512,\n" +
                "                \"type\" : \"keyword\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"message_fields\" : {\n" +
                "              \"match\" : \"message\",\n" +
                "              \"match_mapping_type\" : \"string\",\n" +
                "              \"mapping\" : {\n" +
                "                \"doc_values\" : false,\n" +
                "                \"ignore_above\" : 2048,\n" +
                "                \"index\" : \"false\",\n" +
                "                \"type\" : \"keyword\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"string_fields\" : {\n" +
                "              \"match\" : \"*\",\n" +
                "              \"match_mapping_type\" : \"string\",\n" +
                "              \"mapping\" : {\n" +
                "                \"ignore_above\" : 512,\n" +
                "                \"type\" : \"keyword\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"timestampField\" : {\n" +
                "              \"match\" : \"*imestamp*\",\n" +
                "              \"match_mapping_type\" : \"long\",\n" +
                "              \"mapping\" : {\n" +
                "                \"format\" : \"yyyy-MM-dd HH:mm:ss Z||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS Z||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss,SSS||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd HH:mm:ss,SSS Z||yyyy/MM/dd HH:mm:ss,SSS Z||epoch_millis\",\n" +
                "                \"type\" : \"date\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"logTimeField\" : {\n" +
                "              \"match\" : \"logTime\",\n" +
                "              \"match_mapping_type\" : \"long\",\n" +
                "              \"mapping\" : {\n" +
                "                \"format\" : \"yyyy-MM-dd HH:mm:ss Z||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS Z||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss,SSS||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd HH:mm:ss,SSS Z||yyyy/MM/dd HH:mm:ss,SSS Z||epoch_millis\",\n" +
                "                \"type\" : \"date\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"sinkTimeField\" : {\n" +
                "              \"match\" : \"sinkTime\",\n" +
                "              \"match_mapping_type\" : \"long\",\n" +
                "              \"mapping\" : {\n" +
                "                \"format\" : \"yyyy-MM-dd HH:mm:ss Z||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS Z||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss,SSS||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd HH:mm:ss,SSS Z||yyyy/MM/dd HH:mm:ss,SSS Z||epoch_millis\",\n" +
                "                \"type\" : \"date\"\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        ],\n" +
                "        \"numeric_detection\" : false\n" +
                "      },\n" +
                "      \"_doc\" : {\n" +
                "        \"_all\" : {\n" +
                "          \"enabled\" : false\n" +
                "        },\n" +
                "        \"dynamic_date_formats\" : [\n" +
                "          \"strict_date_optional_time\",\n" +
                "          \"yyyy-MM-dd HH:mm:ss Z||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS Z||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss,SSS||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd HH:mm:ss,SSS Z||yyyy/MM/dd HH:mm:ss,SSS Z||epoch_millis\"\n" +
                "        ],\n" +
                "        \"dynamic_templates\" : [\n" +
                "          {\n" +
                "            \"extractLevel_fields\" : {\n" +
                "              \"match\" : \"extractLevel\",\n" +
                "              \"match_mapping_type\" : \"string\",\n" +
                "              \"mapping\" : {\n" +
                "                \"ignore_above\" : 512,\n" +
                "                \"type\" : \"keyword\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"message_fields\" : {\n" +
                "              \"match\" : \"message\",\n" +
                "              \"match_mapping_type\" : \"string\",\n" +
                "              \"mapping\" : {\n" +
                "                \"doc_values\" : false,\n" +
                "                \"ignore_above\" : 2048,\n" +
                "                \"index\" : \"false\",\n" +
                "                \"type\" : \"keyword\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"string_fields\" : {\n" +
                "              \"match\" : \"*\",\n" +
                "              \"match_mapping_type\" : \"string\",\n" +
                "              \"mapping\" : {\n" +
                "                \"ignore_above\" : 512,\n" +
                "                \"type\" : \"keyword\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"timestampField\" : {\n" +
                "              \"match\" : \"*imestamp*\",\n" +
                "              \"match_mapping_type\" : \"long\",\n" +
                "              \"mapping\" : {\n" +
                "                \"format\" : \"yyyy-MM-dd HH:mm:ss Z||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS Z||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss,SSS||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd HH:mm:ss,SSS Z||yyyy/MM/dd HH:mm:ss,SSS Z||epoch_millis\",\n" +
                "                \"type\" : \"date\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"logTimeField\" : {\n" +
                "              \"match\" : \"logTime\",\n" +
                "              \"match_mapping_type\" : \"long\",\n" +
                "              \"mapping\" : {\n" +
                "                \"format\" : \"yyyy-MM-dd HH:mm:ss Z||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS Z||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss,SSS||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd HH:mm:ss,SSS Z||yyyy/MM/dd HH:mm:ss,SSS Z||epoch_millis\",\n" +
                "                \"type\" : \"date\"\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"sinkTimeField\" : {\n" +
                "              \"match\" : \"sinkTime\",\n" +
                "              \"match_mapping_type\" : \"long\",\n" +
                "              \"mapping\" : {\n" +
                "                \"format\" : \"yyyy-MM-dd HH:mm:ss Z||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSS Z||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss,SSS||yyyy/MM/dd HH:mm:ss||yyyy-MM-dd HH:mm:ss,SSS Z||yyyy/MM/dd HH:mm:ss,SSS Z||epoch_millis\",\n" +
                "                \"type\" : \"date\"\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        ],\n" +
                "        \"numeric_detection\" : false,\n" +
                "        \"properties\" : {\n" +
                "          \"alg_has_driver_license\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"alg_location_3km_gas_store_cnt\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"alg_location_5km_gas_store_cnt\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"alg_location_city\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"alg_location_geo_point\" : {\n" +
                "            \"type\" : \"geo_point\"\n" +
                "          },\n" +
                "          \"alg_mta_mrkt_type\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"base_auth_status\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"base_driver_license_lel\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"base_driver_license_status\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"base_driver_type\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"base_face_status\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"base_reg_city_id\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_break_visit_days_30d\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_break_visit_days_60d\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_break_visit_days_90d\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_break_visit_last_days\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_is_30d_daiban\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_is_30d_ershouche\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_is_30d_weizhang\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_visit_days_30d\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_visit_days_60d\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_visit_days_90d\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carlife_visit_last_days\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carshare_avg_actual_pay_14d\" : {\n" +
                "            \"type\" : \"double\"\n" +
                "          },\n" +
                "          \"carshare_avg_actual_pay_30d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_avg_actual_pay_7d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_avg_gmv_14d\" : {\n" +
                "            \"type\" : \"double\"\n" +
                "          },\n" +
                "          \"carshare_avg_gmv_30d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_avg_gmv_7d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_complete_orders_14d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_complete_orders_30d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_complete_orders_7d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_complete_orders_total\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"carshare_first_finished_city_id\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_first_finished_time\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_is_bindcoupon_unverified_3d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_is_bindcoupon_unverified_8d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_is_navigation_bubble_60d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_is_nearby_station\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_last_bubble_days\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_last_finished_city_id\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_last_finished_days\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_last_finished_time\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_navigation_bubble_14d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_navigation_bubble_30d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_navigation_bubble_7d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_order_cvr_tag\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_push_nums_14d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_push_nums_30d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_push_nums_7d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_sms_nums_14d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_sms_nums_30d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"carshare_sms_nums_7d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"gas_1st_time\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"gas_last_time\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"gas_order_ct_total\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"mta_b2c_order_cnt_total\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"mta_instore_1st_time\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"mta_instore_ct_total\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"mta_instore_last_time\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"pass_age_level\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"pass_days_since_last_finish_order_fast\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"pass_has_add_car\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"pass_has_car_probability\" : {\n" +
                "            \"type\" : \"double\"\n" +
                "          },\n" +
                "          \"pass_native_start_app_cnt_14d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"pass_native_start_app_cnt_30d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"pass_total_finish_orders\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"pass_total_finish_orders_90d\" : {\n" +
                "            \"type\" : \"integer\"\n" +
                "          },\n" +
                "          \"passport_uid\" : {\n" +
                "            \"type\" : \"keyword\"\n" +
                "          },\n" +
                "          \"pid_last_number\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_age_level\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_days_since_last_finish_order_fast\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_has_add_car\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_has_car_probability\" : {\n" +
                "            \"type\" : \"keyword\",\n" +
                "            \"ignore_above\" : 512\n" +
                "          },\n" +
                "          \"psgr_is_carshare_competitive_user\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_is_gf_driver\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_is_gf_long_order_user\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_is_gf_needs_higher_user\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_is_student\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_is_valet_driver\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_is_valet_user\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_last_up_city_id\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_level_id\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_native_start_app_cnt_14d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_native_start_app_cnt_30d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_native_start_app_cnt_7d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_total_finish_orders\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_total_finish_orders_30d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_total_finish_orders_60d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          },\n" +
                "          \"psgr_total_finish_orders_90d\" : {\n" +
                "            \"type\" : \"long\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"settings\" : {\n" +
                "      \"index\" : {\n" +
                "        \"mapping\" : {\n" +
                "          \"ignore_malformed\" : \"true\"\n" +
                "        },\n" +
                "        \"refresh_interval\" : \"30s\",\n" +
                "        \"translog\" : {\n" +
                "          \"durability\" : \"async\"\n" +
                "        },\n" +
                "        \"provided_name\" : \"fast_index_am_dw_ads_tag_pub_passenger_df_2019-07-12\",\n" +
                "        \"creation_date\" : \"1562829600801\",\n" +
                "        \"requests\" : {\n" +
                "          \"cache\" : {\n" +
                "            \"enable\" : \"true\"\n" +
                "          }\n" +
                "        },\n" +
                "        \"unassigned\" : {\n" +
                "          \"node_left\" : {\n" +
                "            \"delayed_timeout\" : \"6h\"\n" +
                "          }\n" +
                "        },\n" +
                "        \"priority\" : \"5\",\n" +
                "        \"number_of_replicas\" : \"0\",\n" +
                "        \"uuid\" : \"TbkP27kJQEOEGm4_V3qSKQ\",\n" +
                "        \"version\" : {\n" +
                "          \"created\" : \"6060199\"\n" +
                "        },\n" +
                "        \"codec\" : \"best_compression\",\n" +
                "        \"routing\" : {\n" +
                "          \"allocation\" : {\n" +
                "            \"include\" : {\n" +
                "              \"rack\" : \"r3,r4\"\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"search\" : {\n" +
                "          \"slowlog\" : {\n" +
                "            \"threshold\" : {\n" +
                "              \"fetch\" : {\n" +
                "                \"warn\" : \"1s\",\n" +
                "                \"trace\" : \"200ms\",\n" +
                "                \"debug\" : \"500ms\",\n" +
                "                \"info\" : \"800ms\"\n" +
                "              },\n" +
                "              \"query\" : {\n" +
                "                \"warn\" : \"10s\",\n" +
                "                \"trace\" : \"500ms\",\n" +
                "                \"debug\" : \"1s\",\n" +
                "                \"info\" : \"5s\"\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"number_of_shards\" : \"8\",\n" +
                "        \"merge\" : {\n" +
                "          \"scheduler\" : {\n" +
                "            \"max_thread_count\" : \"1\"\n" +
                "          }\n" +
                "        },\n" +
                "        \"optimize_auto_generated_id\" : \"true\"\n" +
                "      }\n" +
                "    }\n" +
                "  }";

        IndexInfo indexInfo = new IndexInfo();
        indexInfo.setReducerNum(1);
        indexInfo.setSetting(JSON.parseObject(str));

        IndexConfig indexConfig = new IndexConfig(indexInfo.getSetting());

        Map<String, String> settings = indexConfig.getSettings();
        settings.remove("index.creation_date");
        settings.remove("index.provided_name");
        settings.remove("index.uuid");
        settings.remove("index.version.created");
        settings.remove("index.routing.allocation.include.rack");


        indexConfig.setSettings(settings);
        indexInfo.setSetting(indexConfig.toJson());
        indexInfo.setType("type");

        taskConfig.setBatchSize(1);
        ESNode node = new ESNode(taskConfig, indexInfo);
        node.start();

        ESClient esClient = node.getEsClient();

        String dataStr = "{\n" +
                "          \"alg_has_driver_license\": 0,\n" +
                "          \"alg_location_3km_gas_store_cnt\": 0,\n" +
                "          \"alg_location_5km_gas_store_cnt\": 0,\n" +
                "          \"alg_location_city\": 4,\n" +
                "          \"alg_location_geo_point\": {\n" +
                "            \"lat\": 30.8913,\n" +
                "            \"lon\": 121.73766\n" +
                "          },\n" +
                "          \"alg_mta_mrkt_type\": -1,\n" +
                "          \"base_auth_status\": 0,\n" +
                "          \"base_driver_license_lel\": -1,\n" +
                "          \"base_driver_license_status\": 0,\n" +
                "          \"base_driver_type\": 0,\n" +
                "          \"base_face_status\": 0,\n" +
                "          \"base_reg_city_id\": 4,\n" +
                "          \"carlife_break_visit_days_30d\": 0,\n" +
                "          \"carlife_break_visit_days_60d\": 0,\n" +
                "          \"carlife_break_visit_days_90d\": 0,\n" +
                "          \"carlife_break_visit_last_days\": null,\n" +
                "          \"carlife_is_30d_daiban\": -1,\n" +
                "          \"carlife_is_30d_ershouche\": -1,\n" +
                "          \"carlife_is_30d_weizhang\": -1,\n" +
                "          \"carlife_visit_days_30d\": 0,\n" +
                "          \"carlife_visit_days_60d\": 0,\n" +
                "          \"carlife_visit_days_90d\": 0,\n" +
                "          \"carlife_visit_last_days\": null,\n" +
                "          \"carshare_complete_orders_total\": 0,\n" +
                "          \"carshare_finished_rent_order_cnt\": 0,\n" +
                "          \"carshare_first_finished_time\": null,\n" +
                "          \"carshare_frequently_order_scene\": -1,\n" +
                "          \"carshare_is_bindcoupon_unverified_3d\": 1000.111,\n" +
                "          \"carshare_is_bindcoupon_unverified_8d\": 0,\n" +
                "          \"carshare_is_carrent_ck\": 0,\n" +
                "          \"carshare_is_create_rent_order_30d\": 1000.1,\n" +
                "          \"carshare_is_expire_coupon_4d\": 0,\n" +
                "          \"carshare_is_navigation_bubble_60d\": 0,\n" +
                "          \"carshare_is_nearby_station\": 0,\n" +
                "          \"carshare_is_selectthiscar_ck\": 0,\n" +
                "          \"carshare_last_finished_time\": null,\n" +
                "          \"carshare_navigation_bubble_7d\": null,\n" +
                "          \"gas_1st_time\": null,\n" +
                "          \"gas_last_time\": null,\n" +
                "          \"gas_order_ct_total\": 0,\n" +
                "          \"gas_psxcx_order_ct_total\": 0,\n" +
                "          \"mta_b2c_order_cnt_total\": 0,\n" +
                "          \"mta_instore_1st_time\": null,\n" +
                "          \"mta_instore_ct_total\": 0,\n" +
                "          \"mta_instore_last_time\": null,\n" +
                "          \"passport_uid\": \"282963979415510\",\n" +
                "          \"pid_last_number\": 10,\n" +
                "          \"psgr_age_level\": 4,\n" +
                "          \"psgr_days_since_last_finish_order_fast\": 1014,\n" +
                "          \"psgr_has_add_car\": 0,\n" +
                "          \"psgr_has_car_probability\": 0,\n" +
                "          \"psgr_is_carshare_competitive_user\": 0,\n" +
                "          \"psgr_is_carshare_rent_competitive_user\": 0,\n" +
                "          \"psgr_is_gf_driver\": 0,\n" +
                "          \"psgr_is_gf_needs_higher_user\": 0,\n" +
                "          \"psgr_is_student\": 0,\n" +
                "          \"psgr_is_tourist\": 0.154738,\n" +
                "          \"psgr_is_valet_driver\": 0,\n" +
                "          \"psgr_is_valet_user\": 0,\n" +
                "          \"psgr_level_id\": 0,\n" +
                "          \"psgr_native_start_app_cnt_14d\": 0,\n" +
                "          \"psgr_native_start_app_cnt_30d\": 0,\n" +
                "          \"psgr_native_start_app_cnt_7d\": 0,\n" +
                "          \"psgr_total_finish_orders\": 4,\n" +
                "          \"psgr_total_finish_orders_30d\": 0,\n" +
                "          \"psgr_total_finish_orders_60d\": 0,\n" +
                "          \"psgr_total_finish_orders_90d\": 0\n" +
                "        }";

        JSONObject data = JSON.parseObject(dataStr);
        long start = System.currentTimeMillis();
        EsWriter esWriter = new EsWriter(esClient, taskConfig.getBatchSize(), taskConfig.getThreadPoolSize());
        // 写入
        for(int i=0; i<10; i++) {
            esWriter.bulk(""+i, data);
        }
        esWriter.finish();
        long end = System.currentTimeMillis();
        LogUtils.info(end-start + "ms");



        esClient.refresh();

        esClient.flush();

        esClient.getSetting();

        esClient.forceMerge();


        JSONObject maping = esClient.getMapping();

        node.stop();
    }
}
