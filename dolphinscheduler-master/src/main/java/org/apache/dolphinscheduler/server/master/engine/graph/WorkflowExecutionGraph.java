/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.engine.graph;

import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.task.LoopLogicTaskChannelFactory;
import org.apache.dolphinscheduler.plugin.task.api.utils.TaskTypeUtils;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkflowExecutionGraph implements IWorkflowExecutionGraph {

    private final Map<String, ITaskExecutionRunnable> totalTaskExecuteRunnableMap;

    private final Set<String> failureTaskChains;

    private final Set<String> pausedTaskChains;

    private final Set<String> killedTaskChains;

    private final Set<String> skippedTask;

    private final Map<String, Set<String>> predecessors;

    private final Map<String, Set<String>> successors;

    private final Set<String> activeTaskExecutionRunnable;

    private final Set<String> inActiveTaskExecutionRunnable;

    private final Map<String, Set<String>> loopBodies;

    public WorkflowExecutionGraph() {
        this.failureTaskChains = new HashSet<>();
        this.pausedTaskChains = new HashSet<>();
        this.killedTaskChains = new HashSet<>();
        this.skippedTask = new HashSet<>();
        this.predecessors = new HashMap<>();
        this.successors = new HashMap<>();
        this.totalTaskExecuteRunnableMap = new HashMap<>();
        this.activeTaskExecutionRunnable = new HashSet<>();
        this.inActiveTaskExecutionRunnable = new HashSet<>();
        this.loopBodies = new HashMap<>();
    }

    @Override
    public void addNode(final ITaskExecutionRunnable taskExecutionRunnable) {
        totalTaskExecuteRunnableMap.put(taskExecutionRunnable.getName(), taskExecutionRunnable);
        predecessors.computeIfAbsent(taskExecutionRunnable.getName(), k -> new HashSet<>());
        successors.computeIfAbsent(taskExecutionRunnable.getName(), k -> new HashSet<>());

        if (taskExecutionRunnable.getTaskDefinition().getTaskType().equals(LoopLogicTaskChannelFactory.NAME)) {
            loopBodies.computeIfAbsent(taskExecutionRunnable.getName(), k -> new HashSet<>());
        }
    }

    @Override
    public void addEdge(String fromTaskName, Set<String> toTaskNames) {
        successors.computeIfAbsent(fromTaskName, k -> new HashSet<>()).addAll(toTaskNames);
        toTaskNames.forEach(toTask -> predecessors.computeIfAbsent(toTask, k -> new HashSet<>()).add(fromTaskName));

        // if the task is a loop body, add the edge to the loop body
        ITaskExecutionRunnable taskExecutionRunnable = getTaskExecutionRunnableByName(fromTaskName);
        if (taskExecutionRunnable.getTaskDefinition().getTaskType().equals(LoopLogicTaskChannelFactory.NAME)) {
            // get loop node paramter
            List param = taskExecutionRunnable.getTaskDefinition().getTaskParamList();
            loopBodies.computeIfAbsent(fromTaskName, k -> new HashSet<>()).addAll(toTaskNames);
        }
    }

    @Override
    public List<ITaskExecutionRunnable> getStartNodes() {
        return totalTaskExecuteRunnableMap.values()
                .stream()
                .filter(taskExecutionRunnable -> CollectionUtils
                        .isEmpty(predecessors.get(taskExecutionRunnable.getName())))
                .collect(Collectors.toList());
    }

    @Override
    public List<ITaskExecutionRunnable> getPredecessors(final String taskName) {
        if (!predecessors.containsKey(taskName)) {
            throw new IllegalArgumentException("Cannot find the task: " + taskName + " in graph");
        }
        return predecessors
                .get(taskName)
                .stream()
                .map(this::getTaskExecutionRunnableByName)
                .collect(Collectors.toList());
    }

    @Override
    public List<ITaskExecutionRunnable> getSuccessors(final String taskName) {
        if (!successors.containsKey(taskName)) {
            throw new IllegalArgumentException("Cannot find the task code in graph");
        }
        List<ITaskExecutionRunnable> suc = successors
                .get(taskName)
                .stream()
                .map(this::getTaskExecutionRunnableByName)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(suc)) {
            // if the task has no successors, it could be a loop body node
            // we need to recursively search from this node all the way back to the start node
            // and return the first loop node as the successor
            String loopNode = getNearestLoopNode(taskName);
            if (loopNode != null) {
                ITaskExecutionRunnable loopTask = getTaskExecutionRunnableByName(loopNode);
                if (loopTask != null) {
                    return List.of(loopTask);
                }
            }
        }
        return suc;
    }

    private String getNearestLoopNode(final String taskName) {
        ITaskExecutionRunnable taskExecutionRunnable = getTaskExecutionRunnableByName(taskName);
        final String taskType = taskExecutionRunnable.getTaskDefinition().getTaskType();
        if (taskType.equals("CONDITIONS")) {
            activeTaskExecutionRunnable.remove(taskName);
            removeAllSuccessorsFromInActiveList(taskName);
            return taskName;
        }
        for (String predecessor : predecessors.get(taskName)) {
            String loopNode = getNearestLoopNode(predecessor);
            if (loopNode != null) {
                return loopNode;
            }
        }
        return null;
    }

    // private boolean isLoopDone(final String taskName, final String loopNodeName) {
    //     ITaskExecutionRunnable taskExecutionRunnable = getTaskExecutionRunnableByName(taskName);
    //     ITaskExecutionRunnable loopNode = getTaskExecutionRunnableByName(loopNodeName);
    //     final String taskType = taskExecutionRunnable.getTaskDefinition().getTaskType();
    //     if (taskType.equals("CONDITIONS")) {
    //         return true;
    //     }
    //     for (String predecessor : predecessors.get(taskName)) {
    //         if (isLoopDone(predecessor)) {
    //             return true;
    //         }
    //     }
    //     return false;
    // }

    // remove all successors from inactive list
    private void removeAllSuccessorsFromInActiveList(final String taskName) {
        inActiveTaskExecutionRunnable.remove(taskName);
        List<ITaskExecutionRunnable> sucs = successors
                .get(taskName)
                .stream()
                .map(this::getTaskExecutionRunnableByName)
                .collect(Collectors.toList());
        for (ITaskExecutionRunnable successor : sucs) {
            removeAllSuccessorsFromInActiveList(successor.getName());
        }
    }

    @Override
    public List<ITaskExecutionRunnable> getSuccessors(final ITaskExecutionRunnable taskExecutionRunnable) {
        return getSuccessors(taskExecutionRunnable.getName());
    }

    @Override
    public ITaskExecutionRunnable getTaskExecutionRunnableByName(final String taskName) {
        return totalTaskExecuteRunnableMap.get(taskName);
    }

    @Override
    public ITaskExecutionRunnable getTaskExecutionRunnableById(final Integer taskInstanceId) {
        return totalTaskExecuteRunnableMap.values()
                .stream()
                .filter(taskExecutionRunnable -> taskExecutionRunnable.getTaskInstance() != null
                        && taskInstanceId.equals(taskExecutionRunnable.getTaskInstance().getId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public ITaskExecutionRunnable getTaskExecutionRunnableByTaskCode(final Long taskCode) {
        return totalTaskExecuteRunnableMap.values()
                .stream()
                .filter(taskExecutionRunnable -> taskExecutionRunnable.getTaskDefinition() != null
                        && taskCode.equals(taskExecutionRunnable.getTaskDefinition().getCode()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public boolean isTaskExecutionRunnableActive(final ITaskExecutionRunnable taskExecutionRunnable) {
        return activeTaskExecutionRunnable.contains(taskExecutionRunnable.getName());
    }

    @Override
    public boolean isTaskExecutionRunnableInActive(ITaskExecutionRunnable taskExecutionRunnable) {
        return inActiveTaskExecutionRunnable.contains(taskExecutionRunnable.getName());
    }

    @Override
    public boolean isTaskExecutionRunnableKilled(final ITaskExecutionRunnable taskExecutionRunnable) {
        return killedTaskChains.contains(taskExecutionRunnable.getName());
    }

    @Override
    public boolean isTaskExecutionRunnableFailed(ITaskExecutionRunnable taskExecutionRunnable) {
        return failureTaskChains.contains(taskExecutionRunnable.getName());
    }

    @Override
    public boolean isTaskExecutionRunnablePaused(ITaskExecutionRunnable taskExecutionRunnable) {
        return pausedTaskChains.contains(taskExecutionRunnable.getName());
    }

    @Override
    public List<ITaskExecutionRunnable> getActiveTaskExecutionRunnable() {
        return activeTaskExecutionRunnable
                .stream()
                .map(this::getTaskExecutionRunnableByName)
                .collect(Collectors.toList());
    }

    @Override
    public List<ITaskExecutionRunnable> getAllTaskExecutionRunnable() {
        return new ArrayList<>(totalTaskExecuteRunnableMap.values());
    }

    @Override
    public boolean isTriggerConditionMet(final ITaskExecutionRunnable taskExecutionRunnable) {
        if (isTaskExecutionRunnableActive(taskExecutionRunnable)
                || isTaskExecutionRunnableInActive(taskExecutionRunnable)) {
            return false;
        }
        List<ITaskExecutionRunnable> predecessors = getPredecessors(taskExecutionRunnable.getName());
        boolean allMatch = true;
        for(ITaskExecutionRunnable predecessor : predecessors) {
            boolean isPredecessorInActive = isTaskExecutionRunnableInActive(predecessor);
            boolean isPredecessorFailed = isTaskExecutionRunnableFailed(predecessor);
            boolean isPredecessorPaused = isTaskExecutionRunnablePaused(predecessor);
            boolean isPredecessorKilled = isTaskExecutionRunnableKilled(predecessor);

            if (!isPredecessorInActive
                    || isPredecessorFailed
                    || isPredecessorPaused
                    || isPredecessorKilled) {
                allMatch = false;
                break;
            }
        }
        return allMatch;
        // return getPredecessors(taskExecutionRunnable.getName())
        //         .stream()
        //         .allMatch(predecessor -> isTaskExecutionRunnableInActive(predecessor)
        //                 && !isTaskExecutionRunnableFailed(predecessor)
        //                 && !isTaskExecutionRunnablePaused(predecessor)
        //                 && !isTaskExecutionRunnableKilled(predecessor));
    }

    @Override
    public boolean isAllTaskExecutionRunnableChainFinish() {
        return activeTaskExecutionRunnable.isEmpty();
    }

    @Override
    public boolean isAllTaskExecutionRunnableChainSuccess() {
        if (!isAllTaskExecutionRunnableChainFinish()) {
            return false;
        }
        return !isExistFailureTaskExecutionRunnableChain()
                && !isExistPauseTaskExecutionRunnableChain()
                && !isExistKillTaskExecutionRunnableChain();
    }

    @Override
    public boolean isExistFailureTaskExecutionRunnableChain() {
        return CollectionUtils.isNotEmpty(failureTaskChains);
    }

    @Override
    public boolean isExistPauseTaskExecutionRunnableChain() {
        return CollectionUtils.isNotEmpty(pausedTaskChains);
    }

    @Override
    public boolean isExistKillTaskExecutionRunnableChain() {
        return CollectionUtils.isNotEmpty(killedTaskChains);
    }

    @Override
    public void markTaskExecutionRunnableActive(final ITaskExecutionRunnable taskExecutionRunnable) {
        activeTaskExecutionRunnable.add(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskExecutionRunnableInActive(final ITaskExecutionRunnable taskExecutionRunnable) {
        activeTaskExecutionRunnable.remove(taskExecutionRunnable.getName());
        inActiveTaskExecutionRunnable.add(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskExecutionRunnableChainFailure(final ITaskExecutionRunnable taskExecutionRunnable) {
        assertTaskExecutionRunnableState(taskExecutionRunnable, TaskExecutionStatus.FAILURE);
        failureTaskChains.add(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskExecutionRunnableChainPause(final ITaskExecutionRunnable taskExecutionRunnable) {
        assertTaskExecutionRunnableState(taskExecutionRunnable, TaskExecutionStatus.PAUSE);
        pausedTaskChains.add(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskExecutionRunnableChainKill(final ITaskExecutionRunnable taskExecutionRunnable) {
        assertTaskExecutionRunnableState(taskExecutionRunnable, TaskExecutionStatus.KILL);
        killedTaskChains.add(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskSkipped(final ITaskExecutionRunnable taskExecutionRunnable) {
        markTaskSkipped(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskSkipped(final String taskName) {
        skippedTask.add(taskName);
    }

    @Override
    public boolean isEndOfTaskChain(final ITaskExecutionRunnable taskExecutionRunnable) {
        String taskName = taskExecutionRunnable.getName();
        // Check if there are no successors
        boolean noSuccessors = successors.get(taskName).isEmpty();
        
        // If no direct successors, check for loop condition
        if (noSuccessors) {
            String loopNode = getNearestLoopNode(taskName);
            if (loopNode != null) {
                return false; // Not end of chain if it's part of a loop
            }
        }
        
        return noSuccessors
                || isTaskExecutionRunnableKilled(taskExecutionRunnable)
                || isTaskExecutionRunnablePaused(taskExecutionRunnable)
                || isTaskExecutionRunnableFailed(taskExecutionRunnable);
    }

    @Override
    public boolean isTaskExecutionRunnableSkipped(final ITaskExecutionRunnable taskExecutionRunnable) {
        return skippedTask.contains(taskExecutionRunnable.getName());
    }

    @Override
    public boolean isTaskExecutionRunnableForbidden(final ITaskExecutionRunnable taskExecutionRunnable) {
        return (taskExecutionRunnable.getTaskDefinition().getFlag() == Flag.NO);
    }

    @Override
    public boolean isTaskExecutionRunnableRetrying(final ITaskExecutionRunnable taskExecutionRunnable) {
        if (!taskExecutionRunnable.isTaskInstanceInitialized()) {
            return false;
        }
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        return taskInstance.getState() == TaskExecutionStatus.FAILURE && taskExecutionRunnable.isTaskInstanceCanRetry()
                && isTaskExecutionRunnableActive(taskExecutionRunnable);
    }

    /**
     * Whether all predecessors are skipped.
     * <p> Only when all predecessors are skipped, will return true. If the given task doesn't have any predecessors, will return false.
     */
    @Override
    public boolean isAllPredecessorsSkipped(final ITaskExecutionRunnable taskExecutionRunnable) {
        final List<ITaskExecutionRunnable> predecessors = getPredecessors(taskExecutionRunnable.getName());
        if (CollectionUtils.isEmpty(predecessors)) {
            return false;
        }
        return CollectionUtils.isEmpty(predecessors)
                || predecessors.stream().allMatch(this::isTaskExecutionRunnableSkipped);
    }

    @Override
    public boolean isAllSuccessorsAreConditionTask(final ITaskExecutionRunnable taskExecutionRunnable) {
        final List<ITaskExecutionRunnable> successors = getSuccessors(taskExecutionRunnable.getName());
        if (CollectionUtils.isEmpty(successors)) {
            return false;
        }
        return successors.stream().allMatch(
                successor -> isTaskExecutionRunnableSkipped(successor)
                        || TaskTypeUtils.isConditionTask(taskExecutionRunnable.getTaskInstance().getTaskType()));
    }

    private void assertTaskExecutionRunnableState(final ITaskExecutionRunnable taskExecutionRunnable,
                                                  final TaskExecutionStatus taskExecutionStatus) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        if (taskInstance.getState() == taskExecutionStatus) {
            return;
        }
        throw new IllegalStateException(
                "The task: " + taskExecutionRunnable.getName() + " state: " + taskInstance.getState() + " is not "
                        + taskExecutionStatus);
    }

}
