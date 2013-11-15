package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import org.hamcrest.text.StringContainsInOrder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class ListQueuesTest {


    @Mock private AmazonSQSClient client;


    @Before
    public void setup() throws Exception { }

    @Test
    public void listAFewQueues() throws Exception {
        ListQueuesResult result = new ListQueuesResult().withQueueUrls("a", "b", "c");

        when(client.listQueues(any(ListQueuesRequest.class))).thenReturn(result);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        ListQueues.listQueues(out, client);

        List<String> expected = ImmutableList.of("a", "b", "c");
        assertThat(new String(baos.toByteArray()), StringContainsInOrder.stringContainsInOrder(expected));
    }

    @Test
    public void listALotOfQueues() throws Exception {

        List<String> results1000 = Lists.newArrayList();
        List<String> resultsa = Lists.newArrayList();
        List<String> resultsb = Lists.newArrayList();
        for (int i = 0; i < 999; ++i) {
            results1000.add("a" + i);
            resultsa.add("a" + i);
        }
        results1000.add("b0");
        for (int i = 0; i < 5; ++i) {
            resultsb.add("b" + i);
        }
        final ListQueuesResult result1000 = new ListQueuesResult().withQueueUrls(results1000);
        final ListQueuesResult resulta = new ListQueuesResult().withQueueUrls(resultsa);
        final ListQueuesResult resultb = new ListQueuesResult().withQueueUrls(resultsb);
        final ListQueuesResult resultOther = new ListQueuesResult().withQueueUrls(Lists.<String>newArrayList());

        when(client.listQueues(any(ListQueuesRequest.class))).then(new Answer<ListQueuesResult>() {
            public ListQueuesResult answer(InvocationOnMock invocation) throws Throwable {
                ListQueuesRequest request = (ListQueuesRequest)invocation.getArguments()[0];
                if ("a".equals(request.getQueueNamePrefix())) return resulta;
                if ("b".equals(request.getQueueNamePrefix())) return resultb;
                if ("".equals(request.getQueueNamePrefix())) return result1000;
                return resultOther;
            }
        });

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        ListQueues.listQueues(out, client);

        List<String> expected = Lists.newArrayList(resultsa);
        expected.addAll(resultsb);
        Collections.sort(expected);

        assertThat(new String(baos.toByteArray()), StringContainsInOrder.stringContainsInOrder(expected));
    }

}