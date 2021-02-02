package io.opencmw.server.rest.helper;

import io.opencmw.filter.TimingCtx;
import io.opencmw.serialiser.annotations.MetaInfo;

import de.gsi.dataset.spi.utils.MultiArray;

@MetaInfo(description = "reply type class description", direction = "OUT")
public class ReplyDataType {
    @MetaInfo(description = "ReplyDataType name to show up in the OpenAPI docs")
    public String name;
    public boolean booleanReturnType;
    public byte byteReturnType;
    public short shortReturnType;
    @MetaInfo(description = "a return value", unit = "A", direction = "OUT", groups = { "A", "B" })
    public int intReturnValue;
    public long longReturnValue;
    public byte[] byteArray;
    public MultiArray<double[]> multiArray = MultiArray.wrap(new double[] { 1.0, 2.0, 3.0 }, 0, new int[] { 1 });
    @MetaInfo(description = "WR timing context", direction = "OUT", groups = { "A", "B" })
    public TimingCtx timingCtx;
    @MetaInfo(description = "LSA timing context", direction = "OUT", groups = { "A", "B" })
    public String lsaContext = "";
    @MetaInfo(description = "custom enum reply option", direction = "OUT", groups = { "A", "B" })
    public ReplyOption replyOption = ReplyOption.REPLY_OPTION2;

    public ReplyDataType() {
        // needs default constructor
    }

    @Override
    public String toString() {
        return "ReplyDataType{outputName='" + name + "', returnValue=" + intReturnValue + '}';
    }
}
