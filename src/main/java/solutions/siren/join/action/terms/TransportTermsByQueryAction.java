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
package solutions.siren.join.action.terms;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.SearchService;
import solutions.siren.join.action.terms.collector.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The terms by query transport operation
 */
public class TransportTermsByQueryAction extends TransportBroadcastAction<TermsByQueryRequest, TermsByQueryResponse, TermsByQueryShardRequest, TermsByQueryShardResponse> {

  private final IndicesService indicesService;
  private final ScriptService scriptService;
  private final PageCacheRecycler pageCacheRecycler;
  private final BigArrays bigArrays;
  private final Client client;

  /**
   * Constructor
   */
  @Inject
  public TransportTermsByQueryAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                     TransportService transportService, IndicesService indicesService,
                                     ScriptService scriptService, PageCacheRecycler pageCacheRecycler,
                                     BigArrays bigArrays, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
    super(settings, TermsByQueryAction.NAME, threadPool, clusterService, transportService, actionFilters,
            indexNameExpressionResolver, TermsByQueryRequest.class, TermsByQueryShardRequest.class,
            // Use the generic threadpool which is cached, as we can end up with deadlock with the SEARCH threadpool
            ThreadPool.Names.GENERIC);
    this.indicesService = indicesService;
    this.scriptService = scriptService;
    this.pageCacheRecycler = pageCacheRecycler;
    this.bigArrays = bigArrays;
    this.client = client;
  }

  /**
   * Executes the actions.
   */
  @Override
  protected void doExecute(TermsByQueryRequest request, ActionListener<TermsByQueryResponse> listener) {
    request.nowInMillis(System.currentTimeMillis()); // set time to be used in scripts
    super.doExecute(request, listener);
  }

  /**
   * Creates a new {@link TermsByQueryShardRequest}
   */
  @Override
  protected TermsByQueryShardRequest newShardRequest(int numShards, ShardRouting shard, TermsByQueryRequest request) {
    String[] filteringAliases = indexNameExpressionResolver.filteringAliases(clusterService.state(), shard.index(), request.indices());
    return new TermsByQueryShardRequest(shard.shardId(), filteringAliases, request);
  }

  /**
   * Creates a new {@link TermsByQueryShardResponse}
   */
  @Override
  protected TermsByQueryShardResponse newShardResponse() {
    return new TermsByQueryShardResponse();
  }

  /**
   * The shards this request will execute against.
   */
  @Override
  protected GroupShardsIterator shards(ClusterState clusterState, TermsByQueryRequest request, String[] concreteIndices) {
    Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, request.routing(), request.indices());
    return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, request.preference());
  }

  @Override
  protected ClusterBlockException checkGlobalBlock(ClusterState state, TermsByQueryRequest request) {
    return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
  }

  @Override
  protected ClusterBlockException checkRequestBlock(ClusterState state, TermsByQueryRequest request, String[] concreteIndices) {
    return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
  }

  /**
   * Merges the individual shard responses and returns the final {@link TermsByQueryResponse}.
   */
  @Override
  protected TermsByQueryResponse newResponse(TermsByQueryRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
    int successfulShards = 0;
    int failedShards = 0;
    int numTerms = 0;
    TermsSet[] termsSets = new TermsSet[shardsResponses.length()];
    List<ShardOperationFailedException> shardFailures = null;

    // we check each shard response
    for (int i = 0; i < shardsResponses.length(); i++) {
      Object shardResponse = shardsResponses.get(i);
      if (shardResponse == null) {
        // simply ignore non active shards
      } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
        failedShards++;
        if (shardFailures == null) {
          shardFailures = new ArrayList<>();
        }
        logger.error("Shard operation failed", (BroadcastShardOperationFailedException) shardResponse);
        shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
      } else {
        // on successful shard response, just add to the array or responses so we can process them below
        // we calculate the total number of terms gathered across each shard so we can use it during
        // initialization of the final TermsResponse below (to avoid rehashing during merging)
        TermsByQueryShardResponse shardResp = ((TermsByQueryShardResponse) shardResponse);
        TermsSet terms = shardResp.getTerms();
        termsSets[i] = terms;
        numTerms += terms.size();
        successfulShards++;
      }
    }

    // Merge the responses

    // TermsSet is responsible for the merge, set size to avoid rehashing on certain implementations.
    TermsSet termsSet = TermsSet.newTermsSet(numTerms, request.termsEncoding());
    for (int i = 0; i < termsSets.length; i++) {
      TermsSet terms = termsSets[i];
      if (terms == null) {
        continue;
      }
      termsSet.merge(terms);
    }

    long tookInMillis = System.currentTimeMillis() - request.nowInMillis();
    return new TermsByQueryResponse(termsSet, tookInMillis, shardsResponses.length(), successfulShards, failedShards, shardFailures);
  }

  /**
   * The operation that executes the query and generates a {@link TermsByQueryShardResponse} for each shard.
   */
  @Override
  protected TermsByQueryShardResponse shardOperation(TermsByQueryShardRequest shardRequest) throws ElasticsearchException {
    IndexService indexService = indicesService.indexServiceSafe(shardRequest.shardId().getIndex());
    IndexShard indexShard = indexService.shardSafe(shardRequest.shardId().id());
    TermsByQueryRequest request = shardRequest.request();
    OrderByShardOperation orderByOperation = OrderByShardOperation.get(request.getOrderBy(), request.maxTermsPerShard());

    SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(),
                                                          shardRequest.shardId().getIndex(),
                                                          shardRequest.shardId().id());

    ShardSearchRequest shardSearchRequest = new ShardSearchLocalRequest(request.types(), request.nowInMillis(),
                                                                        shardRequest.filteringAliases());

    SearchContext context = new DefaultSearchContext(0, shardSearchRequest, shardTarget,
      indexShard.acquireSearcher("termsByQuery"), indexService, indexShard, scriptService,
      pageCacheRecycler, bigArrays, threadPool.estimatedTimeInMillisCounter(), parseFieldMatcher,
      SearchService.NO_TIMEOUT);
    SearchContext.setCurrent(context);

    try {
      MappedFieldType fieldType = context.smartNameFieldType(request.field());
      if (fieldType == null) {
        throw new SearchContextException(context, "[termsByQuery] field '" + request.field() +
                "' not found for types " + Arrays.toString(request.types()));
      }

      IndexFieldData indexFieldData = context.fieldData().getForField(fieldType);

      BytesReference querySource = request.querySource();
      if (querySource != null && querySource.length() > 0) {
        XContentParser queryParser = null;
        try {
          queryParser = XContentFactory.xContent(querySource).createParser(querySource);
          QueryParseContext.setTypes(request.types());
          ParsedQuery parsedQuery = orderByOperation.getParsedQuery(queryParser, indexService);
          if (parsedQuery != null) {
            context.parsedQuery(parsedQuery);
          }
        }
        finally {
          QueryParseContext.removeTypes();
          if (queryParser != null) {
            queryParser.close();
          }
        }
      }

      context.preProcess();

      // execute the search only gathering the hit count and bitset for each segment
      logger.debug("{}: Executes search for collecting terms {}", Thread.currentThread().getName(),
        shardRequest.shardId());

      TermsCollector termsCollector = this.getTermsCollector(request.termsEncoding(), indexFieldData, context);
      if (request.maxTermsPerShard() != null) termsCollector.setMaxTerms(request.maxTermsPerShard());
      HitStream hitStream = orderByOperation.getHitStream(context);
      TermsSet terms = termsCollector.collect(hitStream);

      logger.debug("{}: Returns terms response with {} terms for shard {}", Thread.currentThread().getName(),
        terms.size(), shardRequest.shardId());

      return new TermsByQueryShardResponse(shardRequest.shardId(), terms);
    }
    catch (Throwable e) {
      logger.error("[termsByQuery] Error executing shard operation", e);
      throw new QueryPhaseExecutionException(context, "[termsByQuery] Failed to execute query", e);
    }
    finally {
      // this will also release the index searcher
      context.close();
      SearchContext.removeCurrent();
    }
  }

  private TermsCollector getTermsCollector(TermsByQueryRequest.TermsEncoding termsEncoding,
                                           IndexFieldData indexFieldData, SearchContext context) {
    switch (termsEncoding) {
      case LONG:
        return new LongTermsCollector(indexFieldData, context);
      case INTEGER:
        return new IntegerTermsCollector(indexFieldData, context);
      default:
        throw new IllegalArgumentException("[termsByQuery] Invalid terms encoding: " + termsEncoding.name());
    }
  }

  /**
   * Abstraction over the logic of the order by operation.
   */
  private static abstract class OrderByShardOperation {

    protected final Integer maxTermsPerShard;

    private OrderByShardOperation(Integer maxTermsPerShard) {
      this.maxTermsPerShard = maxTermsPerShard;
    }

    /**
     * Returns the {@link ParsedQuery} associated to this order by operation.
     */
    protected ParsedQuery getParsedQuery(final XContentParser queryParser, final IndexService indexService) {
      return indexService.queryParserService().parse(queryParser);
    }

    /**
     * Returns the {@link HitStream} associated to this order by operation.
     */
    protected abstract HitStream getHitStream(final SearchContext context) throws IOException;

    /**
     * Instantiates the appropriate {@link OrderByShardOperation} for the given
     * {@link TermsByQueryRequest.Ordering}.
     * Default to {@link TermsByQueryRequest.Ordering#DEFAULT}.
     */
    private static OrderByShardOperation get(final TermsByQueryRequest.Ordering orderBy, final Integer maxTermsPerShard) {
      // By default, no ordering
      TermsByQueryRequest.Ordering ordering = orderBy != null ? orderBy : TermsByQueryRequest.Ordering.DEFAULT;
      switch (ordering) {
        case DEFAULT:
          return new OrderByDefaultShardOperation(maxTermsPerShard);

        case DOC_SCORE:
          return new OrderByDocScoreShardOperation(maxTermsPerShard);

        default:
          throw new ElasticsearchParseException("[termsByQuery] unknown ordering " + ordering.name());
      }
    }

  }

  /**
   * The default order by operation. Document score will not be computed, and documents will be ordered
   * by their id.
   */
  private static class OrderByDefaultShardOperation extends OrderByShardOperation {

    private OrderByDefaultShardOperation(final Integer maxTermsPerShard) {
      super(maxTermsPerShard);
    }

    @Override
    protected HitStream getHitStream(SearchContext context) throws IOException {
      return new BitSetHitStream(context.query(), context.searcher());
    }

  }

  /**
   * Order by operation based on document score. Document score will be computed, and documents will be ordered
   * by their score.
   */
  private static class OrderByDocScoreShardOperation extends OrderByShardOperation {

    private OrderByDocScoreShardOperation(final Integer maxTermsPerShard) {
      super(maxTermsPerShard);
    }

    @Override
    protected HitStream getHitStream(SearchContext context) throws IOException {
      if (maxTermsPerShard == null) {
        throw new ElasticsearchParseException("[termsByQuery] maxTermsPerShard parameter is null");
      }
      return new TopHitStream(maxTermsPerShard, context.query(), context.searcher());
    }
  }

}
