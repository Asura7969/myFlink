import com.myFlink.java.project.link.bean.SoaLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class BaseTest {

    public static void main(String[] args) {
        TestBean t1 = new TestBean("1", "1", "1");
        TestBean t2 = new TestBean("1", "1", "1");
        TestBean t3 = new TestBean("1", "1", "1");
        TestBean t4 = new TestBean("1", "1", "1");
        TreeMap<String,TestBean> map1 = new TreeMap();
        TreeMap<String,TestBean> map2 = new TreeMap();
        map1.put("1", t1);
        map1.put("2", t4);
        map2.put("1", t2);
        map2.put("2", t3);
        System.out.println(map1.equals(map2));




    }

}
