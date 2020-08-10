package com.dtstack.flinkx.launcher;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FlinkXYarnClientImpl extends YarnClientImpl {

    public List<ApplicationReport> getApplications(Set<String> applicationTypes, EnumSet<YarnApplicationState> applicationStates, Set<String> queue)
            throws YarnException, IOException {
        GetApplicationsRequest getApplicationsRequest = GetApplicationsRequest.newInstance();
        getApplicationsRequest.setApplicationTypes(applicationTypes);
        getApplicationsRequest.setApplicationStates(applicationStates);
        getApplicationsRequest.setQueues(queue);
        return this.rmClient.getApplications(getApplicationsRequest).getApplicationList();
    }

    public List<ApplicationReport> getApplications(String queue) throws IOException, YarnException {
        Set<String> applicationTypes = new HashSet<>();
        applicationTypes.add("Apache Flink");
        EnumSet<YarnApplicationState> applicationStates = EnumSet.noneOf(YarnApplicationState.class);
        applicationStates.add(YarnApplicationState.RUNNING);
        Set<String> queueSet = new HashSet<>();
        queueSet.add(queue);
        return getApplications(applicationTypes, applicationStates, queueSet);
    }

}
