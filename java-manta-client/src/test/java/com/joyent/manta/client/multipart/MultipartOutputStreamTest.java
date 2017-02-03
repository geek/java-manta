package com.joyent.manta.client.multipart;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

@Test
public class MultipartOutputStreamTest {

    public void happyPath() throws Exception {
        ByteArrayOutputStream s1 = new ByteArrayOutputStream();
        ByteArrayOutputStream s2 = new ByteArrayOutputStream();
        ByteArrayOutputStream s3 = new ByteArrayOutputStream();
        MultipartOutputStream mpos = new MultipartOutputStream(16);

        mpos.setNext(s1);
        mpos.write("foo".getBytes("UTF-8"));
        mpos.write("foo".getBytes("UTF-8"));
        Assert.assertEquals(s1.toString("UTF-8"), "");
        mpos.flushBuffer();

        mpos.setNext(s2);
        mpos.write("bar".getBytes("UTF-8"));
        mpos.flushBuffer();

        mpos.setNext(s3);
        mpos.write("baz".getBytes("UTF-8"));
        mpos.flushBuffer();

        Assert.assertEquals(s1.toString("UTF-8"), "foofoo");
        Assert.assertEquals(s2.toString("UTF-8"), "bar");
        Assert.assertEquals(s3.toString("UTF-8"), "baz");
    }

    public void bufSwitchOut() throws Exception {
        ByteArrayOutputStream s1 = new ByteArrayOutputStream();
        ByteArrayOutputStream s2 = new ByteArrayOutputStream();
        MultipartOutputStream mpos = new MultipartOutputStream(16);

        mpos.setNext(s1);
        mpos.write("foo".getBytes("UTF-8"));
        mpos.setNext(s2);
        mpos.flushBuffer();
        Assert.assertEquals(s1.toString("UTF-8"), "");
        Assert.assertEquals(s2.toString("UTF-8"), "foo");
    }

    public void allAligned() throws Exception {
        ByteArrayOutputStream s1 = new ByteArrayOutputStream();
        ByteArrayOutputStream s2 = new ByteArrayOutputStream();
        MultipartOutputStream mpos = new MultipartOutputStream(4);

        mpos.setNext(s1);
        mpos.write("fooo".getBytes("UTF-8"));
        mpos.write("baarbaar".getBytes("UTF-8"));
        mpos.setNext(s2);

        Assert.assertEquals(s1.toString("UTF-8"), "fooobaarbaar");
        Assert.assertEquals(s2.toString("UTF-8"), "");

    }

}
