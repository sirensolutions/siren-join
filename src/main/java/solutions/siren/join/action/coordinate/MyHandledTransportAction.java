package solutions.siren.join.action.coordinate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

/**
 * A TransportAction that self registers a handler into the transport service
 */
public abstract class MyHandledTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends TransportAction<Request,Response> {

  protected MyHandledTransportAction(Settings settings, String actionName, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Class<Request> request) {
    super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver);
    transportService.registerRequestHandler(actionName, request, ThreadPool.Names.GENERIC, new TransportHandler());
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
