import com.myFlink.java.project.link.bean.SoaLog;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class BaseTest {

    public static void main(String[] args) {
        List<SoaLog> l1 = new ArrayList<>();
        l1.add(new SoaLog("1212","1.1","appid","service","iface","method","metric","ipAdress",1L));
        l1.add(new SoaLog("1212","1.1","appid","service","iface","method","metric","ipAdress",1L));

        List<SoaLog> collect = l1.stream().distinct().collect(Collectors.toList());
        for (SoaLog soaLog : collect) {
            System.out.println(soaLog.toString());
        }
    }

    public static class SortedTest implements Comparable<SortedTest> {
        private int age;
        public SortedTest(int age){
            this.age = age;
        }
        public int getAge() {
            return age;
        }
        public void setAge(int age) {
            this.age = age;
        }
        //自定义对象，实现compareTo(T o)方法：
        public int compareTo(SortedTest sortedTest) {
            int num = this.age - sortedTest.getAge();
            //为0时候，两者相同：
            if(num==0){
                return 0;
                //大于0时，传入的参数小：
            }else if(num>0){
                return 1;
                //小于0时，传入的参数大：
            }else{
                return -1;
            }
        }
    }
}
