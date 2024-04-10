import com.nbb.flink.domain.CdcDO;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class Test {


    public static void main(String[] args) {
        TypeInformation.of(CdcDO.class);
    }
}
