/**
 * Copyright (c) 2016, SIREn Solutions. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package solutions.siren.join.action.coordinate;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Abstract class for coordinate search action which enforces {@link XContentType#CBOR} encoding of the content.
 */
public abstract class BaseTransportCoordinateSearchAction<Request extends ActionRequest, Response extends ActionResponse>
extends TransportAction<Request, Response> {

  protected final Client client;
  private final SearchRequestParsers searchRequestParsers;

  protected BaseTransportCoordinateSearchAction(final Settings settings, final String actionName,
                                                final ThreadPool threadPool, final TransportService transportService,
                                                final ActionFilters actionFilters, final IndexNameExpressionResolver indexNameExpressionResolver,
                                                final SearchRequestParsers searchRequestParsers,
                                                final Client client, Supplier<Request> request) {
    super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
    // Use the generic threadpool, as we can end up with deadlock with the SEARCH threadpool
    transportService.registerRequestHandler(actionName, request, ThreadPool.Names.GENERIC, new TransportHandler());
    this.client = client;
    this.searchRequestParsers= searchRequestParsers;
  }

  protected Tuple<XContentType, Map<String, Object>> parseSource(BytesReference source) {
    // nothing to parse...
    if (source == null || source.length() == 0) {
      return null;
    }

    try {
      Tuple<XContentType, Map<String, Object>> parsedSource = XContentHelper.convertToMap(source, false);
      logger.debug("{}: Parsed source: {}", Thread.currentThread().getName(), parsedSource);
      return parsedSource;
    }
    catch (Throwable e) {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        }
        catch (Throwable e1) { /* ignore  */ }
        throw new ElasticsearchParseException("Failed to parse source [" + sSource + "]", e);
    }
  }

  protected SearchSourceBuilder buildSource(XContent content, Map<String, Object> map) {
    try {
      // Enforce the content type to be CBOR as it is more efficient for large byte arrays
      try (XContentBuilder builder = XContentFactory.cborBuilder().map(map)) {
        QueryParseContext context = new QueryParseContext(searchRequestParsers.queryParsers, XContentHelper.createParser(builder.bytes()), parseFieldMatcher);
        return SearchSourceBuilder.fromXContent(context, searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
      }
    }
    catch (IOException e) {
      logger.error("failed to build source", e);
      throw new IllegalStateException("Failed to build source", e);
    }
  }

  class TransportHandler implements TransportRequestHandler<Request> {

    @Override
    public final void messageReceived(final Request request, final TransportChannel channel) throws Exception {
      execute(request, new ActionListener<Response>() {
        @Override
        public void onResponse(Response response) {
          try {
            channel.sendResponse(response);
          } catch (Exception e) {
            onFailure(e);
          }
        }

        @Override
        public void onFailure(Exception e) {
          try {
            channel.sendResponse(e);
          } catch (Exception e1) {
            logger.warn("Failed to send error response for action [{}] and request [{}]", e1,
                    actionName, request);
          }
        }
      });
    }
  }

}
