/**
 * Copyright (c) 2015, SIREn Solutions. All Rights Reserved.
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

/**
 * Abstract class for coordinate search action which enforces {@link XContentType#CBOR} encoding of the content.
 */
public abstract class BaseTransportCoordinateSearchAction<Request extends ActionRequest, Response extends ActionResponse>
extends TransportAction<Request,Response> {

  protected final Client client;

  protected BaseTransportCoordinateSearchAction(final Settings settings, final String actionName,
                                                final ThreadPool threadPool, final TransportService transportService,
                                                final ActionFilters actionFilters,
                                                final IndexNameExpressionResolver indexNameExpressionResolver,
                                                final Client client, Class<Request> request) {
    super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver);
    // Use the generic threadpool, as we can end up with deadlock with the SEARCH threadpool
    transportService.registerRequestHandler(actionName, request, ThreadPool.Names.GENERIC, new TransportHandler());
    this.client = client;
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

  protected XContentBuilder buildSource(XContent content, Map<String, Object> map) {
    try {
      // Enforce the content type to be CBOR as it is more efficient for large byte arrays
      return XContentBuilder.builder(XContentType.CBOR.xContent()).map(map);
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
          } catch (Throwable e) {
            onFailure(e);
          }
        }

        @Override
        public void onFailure(Throwable e) {
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
