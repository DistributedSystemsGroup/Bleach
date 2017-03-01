package storm.dataclean.auxiliary.base;

/**
 * Created by yongchao on 3/1/16.
 */
public interface Windowing {
    boolean updateWindow(int tid);
}
