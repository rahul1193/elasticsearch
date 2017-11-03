/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.monitor.jvm.HotThreads;

import java.util.Timer;
import java.util.TimerTask;

/**
 * author : Sandeep Maheshwari
 * timestamp : 02/11/17
 */
public class SlowActionHotThreadsTracer {

    public static final Logger LOGGER = Loggers.getLogger("slow.action.hot_threads.log");

    private static volatile Timer timer;

    public static TimerTask scheduleOneTimeTask(final ActionRequest request, final long delayMs) {

        final TimerTask task = new TimerTask() {

            private boolean cancelled = false;

            @Override
            public void run() {
                if (cancelled) {
                    return;
                }

                String hotThreads = getHotThreads();
                if (hotThreads == null || hotThreads.isEmpty()) {
                    return;
                }

                StringBuilder sb = new StringBuilder("[\n");
                sb.append("Request:\n").append(request).append("\n").append("Hot Threads::\n")
                    .append(hotThreads).append("\n]");
                LOGGER.warn(sb.toString());
            }

            @Override
            public boolean cancel() {
                this.cancelled = true;
                return super.cancel();
            }
        };

        getOrCreateTimer().schedule(task, delayMs);
        return task;
    }

    private static String getHotThreads() {
        String rv = "";
        try {
            HotThreads hotThreads = new HotThreads().busiestThreads(50).type("cpu");
            rv += "CPU Intensive Threads:\n" + hotThreads.detect() + "\n\n";

            hotThreads = new HotThreads().busiestThreads(50).type("block");
            rv += "Blocked Threads:\n" + hotThreads.detect() + "\n\n";

            hotThreads = new HotThreads().busiestThreads(50).type("wait");
            rv += "Waiting Threads:\n" + hotThreads.detect() + "\n\n";
        } catch (Exception e) {
            //ignore
        }
        return rv;
    }

    private static Timer getOrCreateTimer() {
        if (timer == null) {
            synchronized (SlowActionHotThreadsTracer.class) {
                if (timer == null) {
                    timer = new Timer("SLOW-ACTION-HOT-THREADS-TRACER");
                }
            }
        }
        return timer;
    }
}
