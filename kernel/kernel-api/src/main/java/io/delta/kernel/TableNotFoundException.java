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

package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;

/**
 * Thrown when there is no Delta table at the given location.
 *
 * @since 3.0.0
 */
@Evolving
public class TableNotFoundException
    extends Exception {

    private final String tablePath;

    public TableNotFoundException(String tablePath) {
        this.tablePath = tablePath;
    }

    public TableNotFoundException(String tablePath, Throwable cause) {
        super(cause);
        this.tablePath = tablePath;
    }

    @Override
    public String getMessage() {
        return String.format("Table at path `%s` is not found", tablePath);
    }
}
