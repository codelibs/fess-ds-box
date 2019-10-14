package org.codelibs.fess.ds.box;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;

import org.codelibs.core.misc.Pair;
import org.codelibs.fess.crawler.client.AbstractCrawlerClient;
import org.codelibs.fess.crawler.client.CrawlerClientCreator;
import org.codelibs.fess.crawler.entity.RequestData;
import org.codelibs.fess.crawler.entity.ResponseData;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoxClient extends AbstractCrawlerClient {
    private static final Logger logger = LoggerFactory.getLogger(BoxClient.class);

    protected BoxClientConnection clientConnection;

    private Map<String, String> clientParams = null;

    @PreDestroy
    public void diconnect() {
        if (clientConnection != null) {
            clientConnection.close();
        }
    }
    
    @Override
    public void init() {
        if (clientConnection != null) {
            return;
        }

        super.init();
        // TODO connection pool
        clientConnection = new BoxClientConnection(clientParams);
    }

    @Override
    public ResponseData execute(RequestData data) {
        // TODO process box://...
        return null;
    }

    @Override
    public void setInitParameterMap(final Map<String, Object> params) {
        super.setInitParameterMap(params);
        clientParams = params.entrySet().stream().filter(e -> e.getValue() instanceof String).map(e -> {
            String key = e.getKey();
            if (key.startsWith("box.")) {
                key = key.substring(4);
            }
            return new Pair<>(key, e.getValue().toString());
        }).collect(Collectors.toMap(v -> v.getFirst(), v -> v.getSecond()));
    }

    public void register(final List<String> regexList, final String name) {
        try {
            final CrawlerClientCreator crawlerClientCreator = ComponentUtil.getComponent("crawlerClientCreator");
            regexList.stream().forEach(regex -> crawlerClientCreator.register(regex, name));
        } catch (Exception e) {
            logger.warn("", e);
        }
    }
}
