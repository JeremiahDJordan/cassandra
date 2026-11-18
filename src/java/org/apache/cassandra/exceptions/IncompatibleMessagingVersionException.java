/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.exceptions;

/**
 * Thrown when a message can't be serialized for the messaging version of its destination. The request fails right away
 * for that node (see {@link org.apache.cassandra.net.RequestCallbacks#onFailedSerialize}) without writing a hint, so it
 * must not be thrown when serializing a mutation.
 */
public class IncompatibleMessagingVersionException extends InvalidRequestException
{
    public IncompatibleMessagingVersionException(String msg)
    {
        super(msg);
    }
}
