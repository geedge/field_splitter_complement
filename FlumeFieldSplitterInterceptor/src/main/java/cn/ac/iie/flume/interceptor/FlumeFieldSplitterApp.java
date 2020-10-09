package cn.ac.iie.flume.interceptor;

import cn.ac.iie.flume.interceptor.commonUtils.HashTableConfig;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.IpLookup;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FlumeFieldSplitterApp implements Interceptor {
    private static Logger logger = Logger.getLogger(FlumeFieldSplitterApp.class);

    private static HashMap<String, String> hmFromPz;

    private String handleField;//从原数据中获取---需要进行切分的字段,即最外层的表字段,即"app_label":"PROTO_ID=10062;APP_ID=0;OS_ID=0;BS_ID=0;WEB_ID=0;BEHAV_ID=0;"中的 app_label

    private String splitFields;//用户定义---需要切分的字段的内容字段,key-value形式,key为要补充的名称协议,value为原数据字段名,暂定格式为   proto:PROTO_ID;app:APP_ID;web:WEB_ID;os:OS_ID;bs:BS_ID

    private String fieldsSeparator;//多个字段之间的分隔符,例如"app_label":"PROTO_ID=10062;APP_ID=0;OS_ID=0;BS_ID=0;WEB_ID=0;BEHAV_ID=0;"中的  ;

    private String fieldDelimiter;//单个字段内部的分隔符,例如"app_label":"PROTO_ID=10062;APP_ID=0;OS_ID=0;BS_ID=0;WEB_ID=0;BEHAV_ID=0;"中的  =

    private String defaultFieldValue;//该字段不存在时的默认值value,注意不是json的key,是json的value

    public void initialize() {
        if (!("noSetSplitFields".equals(splitFields))) {
            hmFromPz = new HashMap<>();
            if (splitFields.contains(";")) {
                String[] splitFieldsToKeyValue = splitFields.split(";");
                for (String splitFieldToKeyValue : splitFieldsToKeyValue) {
                    if (splitFieldToKeyValue.contains(":")) {
                        String[] splitSingleFieldToKeyValue = splitFieldToKeyValue.split(":");
                        hmFromPz.put(splitSingleFieldToKeyValue[0], splitSingleFieldToKeyValue[1]);
                    } else {
                        logger.error("key-value must split by ===> : <=== !!!");
                    }
                }
            } else {
                if (splitFields.contains(":")) {
                    String[] splitSingleFieldToKeyValue = splitFields.split(":");
                    hmFromPz.put(splitSingleFieldToKeyValue[0], splitSingleFieldToKeyValue[1]);
                } else {
                    logger.error("field must be split by ===> ; <=== ,and key-value must split by ===> : <=== !!!");
                }
            }
        }

    }

    public FlumeFieldSplitterApp(String handleField, String splitFields, String fieldsSeparator, String fieldDelimiter, String defaultFieldValue) {
        this.handleField = handleField;
        this.splitFields = splitFields;
        this.fieldsSeparator = fieldsSeparator;
        this.fieldDelimiter = fieldDelimiter;
        this.defaultFieldValue = defaultFieldValue;
    }

    public Event intercept(Event event) {
        String message = null;
        try {
            message = new String(event.getBody(), "utf-8");
        } catch (UnsupportedEncodingException e) {
            message = new String(event.getBody());
        }
        try {
            if (StringUtils.isNotBlank(message)) {
                logger.info("Field_Splitter_Interceptor this cycle message begin ---><<" + message + ">><---");
                Map<String, Object> map = JSONObject.parseObject(message, Map.class);
                if (!("noSetHandleField".equals(handleField)) && !("noSetSplitFields".equals(splitFields))
                        && !("noSetFieldsSeparator".equals(fieldsSeparator)) && !("noSetFieldDelimiter".equals(fieldDelimiter))) {
                    /**
                     * 切分存储原数据中需要映射的原始字段
                     */
                    HashMap<String, String> hmInnerFields = new HashMap<>();
                    String handleFieldValue = (String) map.get(handleField);

                    if (StringUtil.isBlank(handleFieldValue)) {
                        handleFieldValue = defaultFieldValue;
                    }

                    String[] splitHandleFieldValues = handleFieldValue.split(fieldsSeparator);
                    for (int i = 0; i < splitHandleFieldValues.length; i++) {
                        if (StringUtils.isNotBlank(splitHandleFieldValues[i])) {
                            String splitHandleFieldValue = splitHandleFieldValues[i];
                            String[] innerFields = splitHandleFieldValue.split(fieldDelimiter);
                            hmInnerFields.put(innerFields[0], innerFields[1]);
                        }
                    }

                    /**
                     * 针对hmInnerFields和hmFromPz开始做映射处理
                     * hmInnerFields格式为hmInnerFields(PROTO_ID,10062)
                     * hmFromPz格式为hmFromPz(proto,PROTO_ID)
                     */
                    //进行协议名称映射
                    if (hmFromPz.containsKey("proto")) {
                        String protoID = hmInnerFields.get(hmFromPz.get("proto"));
                        if (StringUtils.isNotBlank(protoID)) {
                            String protoName = HashTableConfig.PROTOCOL_TYPE.get(protoID);
                            if (StringUtils.isNotBlank(protoName)) {
                                map.put(hmFromPz.get("proto").toLowerCase() + "_name", protoName);
                            }
                        }
                    }

                    //进行应用名称映射
                    if (hmFromPz.containsKey("app")) {
                        String appID = hmInnerFields.get(hmFromPz.get("app"));
                        if (StringUtils.isNotBlank(appID)) {
                            String appName = HashTableConfig.APP_TYPE.get(appID);
                            if (StringUtils.isNotBlank(appName)) {
                                map.put(hmFromPz.get("app").toLowerCase() + "_name", appName);
                            }
                        }
                    }

                    //进行domain映射
                    if (hmFromPz.containsKey("web")) {
                        String webID = hmInnerFields.get(hmFromPz.get("web"));
                        if (StringUtils.isNotBlank(webID)) {
                            String webName = HashTableConfig.DOMAIN_TYPE.get(webID);
                            if (StringUtils.isNotBlank(webName)) {
                                map.put(hmFromPz.get("web").toLowerCase() + "_name", webName);
                            }
                        }
                    }

                    //进行OS映射-操作系统映射-20200630新增-os:OS_ID
                    if (hmFromPz.containsKey("os")) {
                        String osID = hmInnerFields.get(hmFromPz.get("os"));
                        if (StringUtils.isNotBlank(osID)) {
                            String osName = HashTableConfig.OS_TYPE.get(osID);
                            if (StringUtils.isNotBlank(osName)) {
                                map.put(hmFromPz.get("os").toLowerCase() + "_name", osName);
                            }
                        }
                    }

                    //进行BS映射-浏览器映射-20200630新增-bs:BS_ID
                    if (hmFromPz.containsKey("bs")) {
                        String bsID = hmInnerFields.get(hmFromPz.get("bs"));
                        if (StringUtils.isNotBlank(bsID)) {
                            String bsName = HashTableConfig.BS_TYPE.get(bsID);
                            if (StringUtils.isNotBlank(bsName)) {
                                map.put(hmFromPz.get("bs").toLowerCase() + "_name", bsName);
                            }
                        }
                    }
                } else {
                    if ("noSetHandleField".equals(handleField)) {
                        logger.error("Field_Splitter_Interceptor If you want to split,please set handleField!");
                    }
                    if ("noSetSplitFields".equals(splitFields)) {
                        logger.error("Field_Splitter_Interceptor If you want to split,please set splitFields!");
                    }
                    if ("noSetFieldsSeparator".equals(fieldsSeparator)) {
                        logger.error("Field_Splitter_Interceptor If you want to split,please set fieldsSeparator!");
                    }
                    if ("noSetFieldDelimiter".equals(fieldDelimiter)) {
                        logger.error("Field_Splitter_Interceptor If you want to split,please set fieldDelimiter!");
                    }
                }
                message = JSONObject.toJSONString(map);
                logger.info("Field_Splitter_Interceptor  this cycle message over ---><<" + message + ">><---");
                event.setBody(message.getBytes());
                return event;
            }
        } catch (Exception e) {
            logger.error("FlumeFieldSplitterApp intercept(Event event) method is error !!!--->{" + e + "}<---,this message is ===>" + message + "<===");
            e.printStackTrace();
        }
        return event;
    }

    public List<Event> intercept(List<Event> list) {
        List resultList = new ArrayList();
        for (Event event : list) {
            Event r = intercept(event);
            if (r != null) {
                resultList.add(r);
            }
        }
        return resultList;
    }

    public void close() {
    }

    public static class FlumeFieldSplitterAppBuilder implements Interceptor.Builder {

        private String handleField;
        private String splitFields;
        private String fieldsSeparator;
        private String fieldDelimiter;
        private String defaultFieldValue;

        public Interceptor build() {
            return new FlumeFieldSplitterApp(this.handleField, this.splitFields,
                    this.fieldsSeparator, this.fieldDelimiter, this.defaultFieldValue);
        }

        public void configure(Context context) {
            //需要处理的字段,类比于app_label
            this.handleField = context.getString("handleField", "noSetHandleField");

            //切分字段,暂定格式为proto:PROTO_ID;app:APP_ID;web:WEB_ID;os:OS_ID;bs:BS_ID,每组之间以;分割,组内key-value以:分割
            this.splitFields = context.getString("splitFields", "noSetSplitFields");

            //多个字段之间的分隔符,例如"app_label":"PROTO_ID=10062;APP_ID=0;OS_ID=0;BS_ID=0;WEB_ID=0;BEHAV_ID=0;"中的  ;
            this.fieldsSeparator = context.getString("fieldsSeparator", "noSetFieldsSeparator");

            //单个字段内部的分隔符,例如"app_label":"PROTO_ID=10062;APP_ID=0;OS_ID=0;BS_ID=0;WEB_ID=0;BEHAV_ID=0;"中的  =
            this.fieldDelimiter = context.getString("fieldDelimiter", "noSetFieldDelimiter");

            //切分的字段的默认值,比如"app_label":"PROTO_ID=10062;APP_ID=0;OS_ID=0;BS_ID=0;WEB_ID=0;BEHAV_ID=0;"中的 PROTO_ID=10062;APP_ID=0;OS_ID=0;BS_ID=0;WEB_ID=0;BEHAV_ID=0;  部分的默认值
            this.defaultFieldValue = context.getString("defaultFieldValue", "PROTO_ID=0;APP_ID=0;OS_ID=0;BS_ID=0;WEB_ID=0;BEHAV_ID=0;");

            logger.info("FlumeFieldSplitterApp defaultFieldValue is set as : {" + handleField + ":" + defaultFieldValue + "}");
        }
    }
}

