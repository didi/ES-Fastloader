package com.didi.bigdata.mr2es.utils;

public class Constants {

	public static final String MAIN_CLASS = "Hive2ES";

	public static final String KEY_DT = "dt"; //日期
	public static final String KEY_MR_OUTPUT_PATH = "output_mr"; //mr输出路径
	public static final String KEY_ES_OUTPUT_PATH = "output_es"; //es输出路径
	public static final String KEY_REDUCE_NUM = "reduce_num"; //reduce数量
	public static final String KEY_DB = "db"; //hive 库
	public static final String KEY_TABLE = "table"; //hive表
	public static final String KEY_INDEX = "index";
	public static final String KEY_TYPE = "type";
	public static final String KEY_ID = "id";
	public static final String KEY_ES_WORK_DIR = "es_work_dir"; //相对路径,任务执行完成会被清理掉
	public static final String KEY_ES_NODE_NAME = "es_node_name"; //es 节点名
	public static final String KEY_REPLICAS_SHARDS_NUMBER
			= "replicas_shards_number"; //es备份分片
	public static final String KEY_WORKFLOW_NAME = "workflow_name"; //工作流
	public static final String KEY_USER_TYPE = "user_type"; //用户类型
	public static final String KEY_INDEX_CONFIG = "index_config"; //es index 配置


	//tag default value 相关
	public static final String TIME_ZONE_OFFSET = "TIME_ZONE_OFFSET"; //没设值?
	public static final String TAG_DEFAULT_VALUE_URL
			="http://127.0.0.1:8000/bigdata-tagsystem" +
			"/default_feature?user_type=";
	public static final String MAPPER_JSON = "mapper_json"; //mapper json

	//告警相关
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String ODIN_URL
			= "http://127.0.0.1:8080/notify?sys=feature";
	public static final String ALARM_ID = "es fastload 失败: ";

}
