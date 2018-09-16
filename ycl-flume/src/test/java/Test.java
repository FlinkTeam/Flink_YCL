import com.alibaba.fastjson.JSONObject;
import com.run.ycl.parser.file.IndexDesBean;
import com.run.ycl.utils.CacheUtil;
import com.run.ycl.utils.KafkaTopicUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.serialization.LineDeserializer;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.source.SpoolDirectorySource;

import java.nio.charset.CoderResult;
import java.util.regex.Pattern;

public class Test {
    SpoolDirectorySource spoolDirectorySource;

    @org.junit.Test
    public void testTopic() {
        String json = "{\"Z002492\":\"610722896685\",\"Z002150\":\"40016153\",\"Z0027TF\":\"北京市东城区东华门街道东安门大街53号万程华府酒店大厅负一层福慧慈缘\",\"SOURCE_NAME\":\"DELIVERY_SF\",\"B020010\":\"徐磊\",\"DATA_SOURCE\":\"source_999_SF\",\"H030008\":\"1478146612\",\"B020013\":\"郑雯\",\"B020012\":\"安徽省阜阳市颍东区 安徽省阜阳市颍东经济开发区东兴路3号\",\"Z002146\":\"010-51385766\"}";
        JSONObject jsonObj = JSONObject.parseObject(json);
        System.out.println(KafkaTopicUtil.getTopic(jsonObj));
    }

    @org.junit.Test
    public void testCache1() {
        //CacheUtil.updateNonstandardBcpIndexCache("source_999_SF".toUpperCase(), "DELIVERY_SF");

        System.out.println(CacheUtil.getIfPresentFromNonstandardBcpIndex("source_999_SF", "DELIVERY_SF").toString());
    }

    @org.junit.Test
    public void testString() {
        String s = "/usr/local/run/sun/nonstandard_bcp/source_999_SF/shunfeng_33475891_20161123-DELIVERY_SF-1.bcp";
        String[] fileInfos = s.split("/");
        String dataSource = ""; //数据来源
        String sourceName = ""; //协议名称
        for (String fileInfo: fileInfos) {

            System.out.println("before convert,fileInfo:" + fileInfo);

            if (fileInfo.startsWith("source") && !fileInfo.endsWith(".bcp")) {
                dataSource = fileInfo;
            } else if (fileInfo.endsWith(".bcp")) {
                //shunfeng_33475891_20161123-DELIVERY_SF-9.bcp
                String[] fileNameInfos = fileInfo.split("-");
                //暂定取最后一个，后面修改为根据配置获取
                sourceName = fileNameInfos[fileNameInfos.length - 2];
            }
        }

        System.out.println("dataSource:" + dataSource);
        System.out.println("sourceName:" + sourceName);
    }

    @org.junit.Test
    public void testIncludePath() {
        String includePattern = ".*bcp$";
        String fileName = "aaa/aa/aa.bcpcc";
        System.out.println(Pattern.compile(includePattern).matcher(fileName).matches());
    }

    public void testEncode() {
        //CoderResult
        ResettableFileInputStream stream;
        LineDeserializer lineDeserializer;
    }

    @org.junit.Test
    public void testGetSourceName() {
        String fileInfo = "xqpezb20140304-CELLCONFIGURATION-ctab2.csv";
        String dataSource = "SOURCE_999";
        getSourceName(fileInfo, dataSource);
    }

    private void getSourceName(String fileInfo, String dataSource) {
        String sourceName = "";
        if (fileInfo.endsWith(".csv")) {
            //shunfeng_33475891_20161123-DELIVERY_SF-9.bcp
            //根据数据来源名称，从配置中获取协议名称在文件名称中的位置
            //暂定取最后一个，后面修改为根据配置获取
            String dataSourceConfigStr = CacheUtil.getIfPresentFromCommonString("config_" + dataSource);
            JSONObject dataSourceJson = JSONObject.parseObject(dataSourceConfigStr);
            String fileInfoConfig = dataSourceJson.getString("Value");

            System.out.println("convert ing...,fileInfoConfig:" + fileInfoConfig);

            if (StringUtils.isNotBlank(fileInfoConfig)) {
                String[] fileConfigInfo = fileInfoConfig.split(";");
                for (String fileConf: fileConfigInfo) {
                    if (fileConf.startsWith("fileNameInfo")) {
                        String[] fileNameInfo = fileConf.split(":")[1].split("\\|");
                        String fileSeparator, timePosition, sourcePosition;
                        if (fileNameInfo.length > 2) {
                            fileSeparator = fileNameInfo[0];
                            timePosition = fileNameInfo[1];
                            sourcePosition = fileNameInfo[2];

                            String[] nameSplit = fileInfo.split(fileSeparator);

                            int len = nameSplit.length;
                            Integer source_index = Integer.valueOf(sourcePosition) - 1;
                            //如果填写不对，默认在首部
                            if (source_index < 0 || source_index > len - 1){
                                System.out.println("sourcePosition is error");
                                source_index = 0;
                            }
                            sourceName =  nameSplit[source_index];
                            IndexDesBean indexDesBean = CacheUtil.getIfPresentFromNonstandardBcpIndex(dataSource, sourceName);
                            System.out.println(indexDesBean.toString());
                            System.out.println(indexDesBean.getItems().length);
                        } else {
                            System.out.println("conver error, cannot get datasource name.fileInfoConfig:" + fileInfoConfig);
                        }
                    }
                }
            }
        }
    }

    @org.junit.Test
    public void testStringSplit() {
       // String s = "fileNameInfo:-|0|2".split(":")[1].split("\\|");
       // String[] s2 = s.split("\\|");
       // System.out.println(s);

        SpoolDirectorySource spoolDirectorySource;
        String s = "工人体育场东门,,,神路街局,工人体育场东门-A2,41000,16912,116.443674,39.928456,\"北京市北京市朝阳区工人体育场 西门进顶到头向北, 从第22看台口进入体育场内,   机房在19看台顶轻体房\",宏基站,45,爱立信,S44";
        System.out.println(s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").length);
      //  ",(?=([^"]*"[^"]*")*[^"]*$)"
    }
}
