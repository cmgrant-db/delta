/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.skipping;

import java.util.Set;

import io.delta.kernel.expressions.Predicate;

// TODO do we need the referencedStats for stats schema pruning or is there a another way to do it?
public class DataSkippingPredicate {

    public final Predicate predicate;
    public final Set<StatsColumn> referencedStats;

    DataSkippingPredicate(Predicate predicate, Set<StatsColumn> referencedStats) {
        this.predicate = predicate;
        this.referencedStats = referencedStats;
    }
}
