/*!
 * Copyright 2024 Hitachi Vantara.  All rights reserved.
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
 *
 */

package org.pentaho.di.shared;

import java.io.IOException;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.w3c.dom.Node;

/**
 * This interface provide methods to retrieve and save the shared objects defined in shared.xml irrespective of
 * where the shared objects are persisted.
 */
public interface SharedObjectsIO {

  public enum SharedObjectType {
    CONNECTION( "connection" ),
    SLAVESERVER( "slaveserver" ),
    PARTITIONSCHEMA( "partitionschema" ),
    CLUSTERSCHEMA( "clusterschema" );

    private final String name;

    SharedObjectType( String name ) {
      this.name = name;
    }
    public String getName() {
      return name;
    }
  }

  /**
   * Get the collection of SharedObjects of the given type
   * @param type The type is shared object type for example, "connection", "slaveserver", "partitionschema" and clusterschema"
   * @return Map<String,Node>  The collection of name of sharedObject and the Xml Node containing the details. For example,
   *                            {"my-postgres", node} where  my-postgres is the name of
   *                            the type - database connection and the Node is xml node containing the details of the db connection.
   * @throws KettleException
   */
  Map<String, Node> getSharedObjects( String type ) throws KettleException;

  /**
   * Save the SharedObject node for the type and name.
   * @param type The type is shared object type for example, "connection", "slaveserver", "partitionschema" and clusterschema"
   * @param name The name is the name of the sharedObject
   * @param node The Xml node containing the details of the shared object
   * @throws KettleException
   */
  void saveSharedObject( String type, String name, Node node ) throws KettleException;

  /**
   * Return the Shared Object Node for the given type and name
   * @param type The type is shared object type for example, "connection", "slaveserver", "partitionschema" and clusterschema"
   * @param name The name is the name of the sharedObject
   * @return Xml node
   * @throws KettleException
   */
  Node getSharedObject( String type, String name ) throws KettleException;

  /**
   * Delete the SharedObject for the given type and name
   * @param type The type is shared object type for example, "connection", "slaveserver", "partitionschema" and clusterschema"
   * @param name The name is the name of the sharedObject
   * @throws KettleException
   */
  void delete( String type, String name ) throws KettleException;

  /**
   * Remove all the connections of a given type.
   *
   *
   * @param type Type to clear
   */
  void clear( String type ) throws KettleException;

}