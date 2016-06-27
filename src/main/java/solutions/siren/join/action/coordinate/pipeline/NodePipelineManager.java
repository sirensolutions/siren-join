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
package solutions.siren.join.action.coordinate.pipeline;

import java.util.*;

/**
 * The pipeline to execute a sequence of {@link NodeTask}s.
 */
public class NodePipelineManager {

  private Queue<NodeTask> taskQueue;
  private List<NodeTask> tasks;
  private NodePipelineListener listener;

  public NodePipelineManager() {
    this.tasks = new ArrayList<>();
  }

  public void addListener(NodePipelineListener listener) {
    this.listener = listener;
  }

  public NodePipelineListener getListener() {
    return listener;
  }

  public void addTask(NodeTask task) {
    this.tasks.add(task);
  }

  /**
   * Starts the execution of the pipeline
   */
  public void execute(NodeTaskContext context) {
    NodeTaskReporter taskReporter = new NodeTaskReporter(this);
    taskQueue = new ArrayDeque<>(tasks);
    if (!taskQueue.isEmpty()) {
      NodeTask task = taskQueue.poll();
      task.execute(context, taskReporter);
    }
    else { // if the queue is empty, reports that the pipeline execution was a success
      listener.onSuccess();
    }
  }

  /**
   * Executes the next task in the queue
   */
  void execute(NodeTaskContext context, NodeTaskReporter taskReporter) {
    if (!taskQueue.isEmpty()) {
      NodeTask task = taskQueue.poll();
      task.execute(context, taskReporter);
    }
    else { // if the queue is empty, reports that the pipeline execution was a success
      listener.onSuccess();
    }
  }

  /**
   * Force the termination of the pipeline
   */
  public void terminate() {
    taskQueue.clear();
    listener.onSuccess();
  }

}
