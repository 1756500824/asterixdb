/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.operators;

import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.*;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.parser.controller.ChangeFeedParserController;
import org.apache.asterix.external.parser.controller.ChangeFeedWithMetaParserController;
import org.apache.asterix.external.parser.controller.FeedParserController;
import org.apache.asterix.external.parser.controller.FeedWithMetaParserController;
import org.apache.asterix.external.provider.DatasourceFactoryProvider;
import org.apache.asterix.external.provider.ParserFactoryProvider;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * FeedCollectOperatorDescriptor is responsible for ingesting data from an external source. This
 * operator uses a user specified for a built-in adaptor for retrieving data from the external
 * data source.
 */
public class FeedCollectOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    /** The type associated with the ADM data output from (the feed adapter OR the compute operator) */
    private final IAType outputType;

    /** unique identifier for a feed instance. */
    private final FeedConnectionId connectionId;

    /** Map representation of policy parameters */
    private final Map<String, String> feedPolicyProperties;

    /** The subscription location at which the recipient feed receives tuples from the source feed {SOURCE_FEED_INTAKE_STAGE , SOURCE_FEED_COMPUTE_STAGE} **/
    private final FeedRuntimeType subscriptionLocation;

    /** The recordType for dataParserFactory */
    private ARecordType recordType;

    /** The configuration for dataParserFactory */
    private Map<String, String> configuration;

    /** The metaType for dataparserFactory */
    private ARecordType metaType;

    public FeedCollectOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId, ARecordType atype,
            RecordDescriptor rDesc, Map<String, String> feedPolicyProperties, FeedRuntimeType subscriptionLocation) {
        super(spec, 1, 1);
        this.outRecDescs[0] = rDesc;
        this.outputType = atype;
        this.connectionId = feedConnectionId;
        this.feedPolicyProperties = feedPolicyProperties;
        this.subscriptionLocation = subscriptionLocation;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        FeedCollectOperatorNodePushable feedCollect = null;
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        INcApplicationContext appCtx = (INcApplicationContext) serviceCtx.getApplicationContext();
        try {
            IDataParserFactory dataParserFactory =
                    ParserFactoryProvider.getDataParserFactory(appCtx.getLibraryManager(), configuration);
            dataParserFactory.setRecordType(recordType);
            dataParserFactory.setMetaType(metaType);
            dataParserFactory.configure(configuration);
            IRecordDataParserFactory<?> recordParserFactory = (IRecordDataParserFactory<?>) dataParserFactory;
            IRecordDataParser<?> dataParser = recordParserFactory.createRecordParser(ctx);
            FeedParserController feedParserController;
            IExternalDataSourceFactory dataSourceFactory =
                    DatasourceFactoryProvider.getExternalDataSourceFactory(appCtx.getLibraryManager(), configuration);
            // TODO deal with the special case as follow
//            if (dataSourceFactory.isIndexible() && (files != null)) {
//                ((IIndexibleExternalDataSource) dataSourceFactory).setSnapshot(files, indexingOp);
//            }
            dataSourceFactory.configure(serviceCtx, configuration);
            FileSplit[] feedLogFileSplits = FeedUtils.splitsForAdapter((ICcApplicationContext) appCtx,
                    ExternalDataUtils.getDataverse(configuration), ExternalDataUtils.getFeedName(configuration),
                    dataSourceFactory.getPartitionConstraint());
            FeedLogManager feedLogManager = FeedUtils.getFeedLogManager(ctx, partition, feedLogFileSplits);
            feedLogManager.touch();
            boolean isChangeFeed = ExternalDataUtils.isChangeFeed(configuration);
            boolean isRecordWithMeta = ExternalDataUtils.isRecordWithMeta(configuration);
            if (isRecordWithMeta) {
                if (isChangeFeed) {
                    feedParserController =
                            new ChangeFeedWithMetaParserController((IRecordWithMetadataParser) dataParser);
                } else {
                    feedParserController = new FeedWithMetaParserController((IRecordWithMetadataParser) dataParser);
                }
            } else if (isChangeFeed) {
                feedParserController = new ChangeFeedParserController((IRecordWithPKDataParser) dataParser);
            } else {
                feedParserController = new FeedParserController(dataParser);
            }
            if (isChangeFeed || isRecordWithMeta) {
                feedCollect = new FeedCollectOperatorNodePushable(ctx, connectionId, feedPolicyProperties, partition,
                        true, feedLogManager, feedParserController);
            } else {
                feedCollect = new FeedCollectOperatorNodePushable(ctx, connectionId, feedPolicyProperties, partition,
                        false, feedLogManager, feedParserController);
            }
        } catch (AlgebricksException e) {
            e.printStackTrace();
            // TODO deal with the exception
        }
        return feedCollect;
    }

    public FeedConnectionId getFeedConnectionId() {
        return connectionId;
    }

    public Map<String, String> getFeedPolicyProperties() {
        return feedPolicyProperties;
    }

    public IAType getOutputType() {
        return outputType;
    }

    public RecordDescriptor getRecordDescriptor() {
        return outRecDescs[0];
    }

    public FeedRuntimeType getSubscriptionLocation() {
        return subscriptionLocation;
    }

    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    public void setMetaType(ARecordType metaType) {
        this.metaType = metaType;
    }

}
