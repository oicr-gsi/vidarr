package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
public class ReadGroupTest {

    @Test
    public void nullThrows(){
        ThrowingRunnable runnable = (() -> new ReadGroup(null, null,null,null,null,null,null,null,null,null,null,null,null,null));
        Assert.assertThrows(NullPointerException.class, runnable);
    }

    @Test
    public void minimalOK(){
        new ReadGroup("ID", null, "CN",null,null,null,null,"LB",null,null,null,null,"PU","SM");
    }

}