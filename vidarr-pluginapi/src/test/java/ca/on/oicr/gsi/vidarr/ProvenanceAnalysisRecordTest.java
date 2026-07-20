package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProvenanceAnalysisRecordTest {
  static ProvenanceAnalysisRecord<ExternalId> oneWithJustId;
  static ProvenanceAnalysisRecord<ExternalKey> oneWithKeys;

  @BeforeClass
  public static void before(){
    oneWithJustId = new ProvenanceAnalysisRecord<>();
    oneWithJustId.setCreated(ZonedDateTime.of(2026,7,20,9,54,0,0,
        ZoneId.of(ZoneId.SHORT_IDS.get("EST"))));
    oneWithJustId.setId("oneWithJustId");

    Map<String,String> labels = new HashMap<>();
    labels.put("the","label");
    oneWithJustId.setLabels(labels);

    oneWithJustId.setChecksum("abcde12345");
    oneWithJustId.setChecksumType("nonsense");
    oneWithJustId.setMetatype("file/nonsense");
    oneWithJustId.setModified(ZonedDateTime.of(2026,7,20,9,54,0,0,
        ZoneId.of(ZoneId.SHORT_IDS.get("EST"))));
    oneWithJustId.setPath("/dev/null");
    oneWithJustId.setSize(1L);
    oneWithJustId.setType("test");
    oneWithJustId.setWorkflowRun("lkjfdslkjfds");

    List<ExternalId> ids = new ArrayList<>();
    ids.add(new ExternalId("provider", "id1"));
    ids.add(new ExternalId("provider", "id2"));
    ids.add(new ExternalId("provider2", "id1"));
    oneWithJustId.setExternalKeys(ids);

    oneWithKeys = new ProvenanceAnalysisRecord<>();
    oneWithKeys.setCreated(ZonedDateTime.of(2026,7,20,9,54,0,0,
        ZoneId.of(ZoneId.SHORT_IDS.get("EST"))));
    oneWithKeys.setId("oneWithKeys");

    Map<String,String> labels2 = new HashMap<>();
    labels2.put("the","label");
    oneWithKeys.setLabels(labels2);

    oneWithKeys.setChecksum("abcde12345");
    oneWithKeys.setChecksumType("nonsense");
    oneWithKeys.setMetatype("file/nonsense");
    oneWithKeys.setModified(ZonedDateTime.of(2026,7,20,9,54,0,0,
        ZoneId.of(ZoneId.SHORT_IDS.get("EST"))));
    oneWithKeys.setPath("/dev/null");
    oneWithKeys.setSize(1L);
    oneWithKeys.setType("test");
    oneWithKeys.setWorkflowRun("lkjfdslkjfds");

    List<ExternalKey> keys = new ArrayList<>();
    Map<String,String> keys1 = new HashMap<>();
    keys1.put("abc", "def");
    Map<String,String> keys2 = new HashMap<>();
    keys2.put("ghi", "jkl");
    Map<String,String> keys3 = new HashMap<>();
    keys3.put("qwe","rty");
    keys.add(new ExternalKey("provider", "id1", keys1));
    keys.add(new ExternalKey("provider", "id2", keys2));
    keys.add(new ExternalKey("provider2", "id1", keys3));
    oneWithKeys.setExternalKeys(keys);
  }

  @Test
  public void differentEquals(){
    Assert.assertEquals(oneWithJustId, oneWithKeys);
  }

  @Test
  public void differentHash(){
    Assert.assertEquals(oneWithJustId.hashCode(), oneWithKeys.hashCode());
  }

  @Test
  public void cloneEquals(){
    ProvenanceAnalysisRecord<ExternalKey> clone = clone();
    Assert.assertEquals(oneWithKeys, clone);
  }

  @Test
  public void cloneHash(){
    ProvenanceAnalysisRecord<ExternalKey> clone = clone();
    Assert.assertEquals(oneWithKeys.hashCode(), clone.hashCode());
  }

  public ProvenanceAnalysisRecord<ExternalKey> clone(){
    ProvenanceAnalysisRecord<ExternalKey> clone = new ProvenanceAnalysisRecord<>();
    clone.setCreated(oneWithKeys.getCreated());
    clone.setExternalKeys(oneWithKeys.getExternalKeys());
    clone.setId(oneWithKeys.getId());
    clone.setLabels(oneWithKeys.getLabels());
    clone.setChecksum(oneWithKeys.getChecksum());
    clone.setChecksumType(oneWithKeys.getChecksumType());
    clone.setMetatype(oneWithKeys.getMetatype());
    clone.setModified(oneWithKeys.getModified());
    clone.setPath(oneWithKeys.getPath());
    clone.setSize(oneWithKeys.getSize());
    clone.setType(oneWithKeys.getType());
    clone.setWorkflowRun(oneWithKeys.getWorkflowRun());
    return clone;
  }
}
