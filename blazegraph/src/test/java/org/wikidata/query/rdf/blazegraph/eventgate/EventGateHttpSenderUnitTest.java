package org.wikidata.query.rdf.blazegraph.eventgate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class EventGateHttpSenderUnitTest {
    @Test
    public void testSendOneEvent() throws IOException {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse resp = mock(CloseableHttpResponse.class);

        when(resp.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
        when(client.execute(any(HttpUriRequest.class))).thenReturn(resp);

        Event event = EventGateTestUtils.newTestEvent();
        EventGateHttpSender sender = new EventGateHttpSender(client, eventGateHost);
        sender.push(event);

        ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);
        verify(client, times(1)).execute(captor.capture());
        verify(resp, times(1)).close();
        HttpUriRequest request = captor.getValue();

        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(request.getURI().toString()).isEqualTo(eventGateHost);
        assertThat(request).isInstanceOf(HttpPost.class);
        HttpPost post = (HttpPost) request;
        assertThat(post.getEntity().getContentType().getValue()).isEqualTo(ContentType.APPLICATION_JSON.toString());
        String json = IOUtils.toString(post.getEntity().getContent());
        assertThat(json).isEqualTo("[" + EventGateTestUtils.testEventJsonString() + "]");
    }

    @Test
    public void testSendMultipleEvents() throws IOException {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse resp = mock(CloseableHttpResponse.class);

        when(resp.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
        when(client.execute(any(HttpUriRequest.class))).thenReturn(resp);

        int n = 0;
        Collection<Event> events = Stream.generate(EventGateTestUtils::newTestEvent)
                .limit(n)
                .collect(Collectors.toList());

        String expectedEventsAsJson = Stream.generate(EventGateTestUtils::testEventJsonString)
                .limit(n)
                .collect(Collectors.joining(",", "[",  "]"));

        EventGateHttpSender sender = new EventGateHttpSender(client, eventGateHost);
        assertThat(sender.push(events)).isTrue();

        ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);
        verify(client, times(1)).execute(captor.capture());
        verify(resp, times(1)).close();
        HttpUriRequest request = captor.getValue();

        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(request.getURI().toString()).isEqualTo(eventGateHost);
        assertThat(request).isInstanceOf(HttpPost.class);
        HttpPost post = (HttpPost) request;
        assertThat(post.getEntity().getContentType().getValue()).isEqualTo(ContentType.APPLICATION_JSON.toString());
        String json = IOUtils.toString(post.getEntity().getContent());
        assertThat(json).isEqualTo(expectedEventsAsJson);
    }

    @Test
    public void testDoesNotBlowUpOnFailure() throws IOException {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        when(client.execute(any(HttpUriRequest.class))).thenThrow(new IOException("failure"));
        EventGateHttpSender sender = new EventGateHttpSender(client, eventGateHost);
        assertThat(sender.push(EventGateTestUtils.newTestEvent())).isFalse();
        assertThat(sender.push(Collections.singleton(EventGateTestUtils.newTestEvent()))).isFalse();
        // we should not fail here
    }

    @Test
    public void testPushReturnsFalseOnHttpErrorCode() throws IOException {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse resp = mock(CloseableHttpResponse.class);

        when(resp.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 404, "Not found"));
        when(client.execute(any(HttpUriRequest.class))).thenReturn(resp);

        EventGateHttpSender sender = new EventGateHttpSender(client, eventGateHost);
        assertThat(sender.push(EventGateTestUtils.newTestEvent())).isFalse();
        assertThat(sender.push(Collections.singleton(EventGateTestUtils.newTestEvent()))).isFalse();

        verify(resp, times(2)).close();
    }

    @Test
    @SuppressWarnings("EmptyBlock")
    public void testClientIsClosed() throws Exception {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        try (EventGateHttpSender sender = new EventGateHttpSender(client, eventGateHost)) {}
        verify(client, times(1)).close();
    }
}
