package windows.day05;

/**
 * EventTime滚动窗口的时间范围边界
 *
 * 1.flink中的所有的时间窗口, 时间精确到毫秒
 * 2.flink中的窗口的起始时间, 结束时间是对齐的( 即窗口的起始时间, 结束时间, 是窗口的整数倍)
 * 3.flink中的窗口范围是前闭后开的范围[5000, 10000] [5000,9999]
 */
public class _15_EventTimeTumblingWindowBound {

    public static void main(String[] args) {
        long event_time = 2345;
        long windowSize = 5000;

        long event_start = event_time - event_time % windowSize;
        long event_end = event_start + windowSize;

        System.out.println(event_time + "对应的窗口时间范围是︰[" + event_start + " , " + event_end +")");

    }
}
