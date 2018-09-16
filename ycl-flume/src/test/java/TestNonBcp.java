import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.CacheUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;

/**
 * Created by jianghui on 18-8-29.
 */
public class TestNonBcp {

    @org.junit.Test
    public void testGetSourceName() {
        String fileInfo = "shunfeng_33475891_20161123-1-DELIVERY_SF-1.bcp";
        String dataSource = "SOURCE_999_SF";
        getSourceName(fileInfo, dataSource);
    }
    private void getSourceName(String fileInfo, String dataSource) {
        String sourceName = "";
        String dataSourceConfigStr = CacheUtil.getIfPresentFromCommonString("config_" + dataSource);
        JSONObject dataSourceJson = JSONObject.parseObject(dataSourceConfigStr);
        String fileInfoConfig = dataSourceJson.getString("Value");
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
                    } else {
                        System.out.println("conver error, cannot get datasource name.fileInfoConfig:" + fileInfoConfig);
                    }
                }
            }
        } else {
            System.out.println("conver error, cannot get datasource name.fileInfoConfig:" + fileInfoConfig);
        }
        System.out.println("sourceName:" +sourceName);
    }
}
