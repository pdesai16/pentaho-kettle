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

import org.pentaho.di.core.bowl.Bowl;
import org.pentaho.di.core.bowl.ManagerFactory;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * This class uses the SharedObjectsIO to retrieve and save shared objects. This is used by the UI.
 * <p>
 * This class caches the state of the underlying SharedObjectsIO, and does not re-read from the source. Only changes
 * written through this interface will be reflected.
 */
public class DatabaseConnectionManager implements DatabaseManagementInterface {
  public  static final String DB_TYPE = SharedObjectsIO.SharedObjectType.CONNECTION.getName();

  private static Class<?> PKG = DatabaseConnectionManager.class; // for i18n purposes, needed by Translator2!!
  private final SharedObjectsIO sharedObjectsIO;
  private Map<String, DatabaseMeta> dbMetas = new HashMap<>();
  private volatile boolean initialized = false;

  public static DatabaseConnectionManager getInstance( Bowl bowl ) throws KettleException {
    return new DatabaseConnectionManager( bowl );
  }

  private DatabaseConnectionManager( Bowl bowl ) throws KettleException {
    this( bowl.getSharedObjectsIO() );
  }

  @VisibleForTesting
  DatabaseConnectionManager( SharedObjectsIO sharedObjectsIO ) {
    this.sharedObjectsIO = sharedObjectsIO;
  }

  /**
   * {@inheritDoc}
   *
   * @return List<DatabaseMeta> Returns a List of DatabaseMeta
   * @throws KettleException
   */
  @Override
  public List<DatabaseMeta> getDatabases() throws KettleException {
    populateDbMetaMap();

    // defensive copies
    return dbMetas.values().stream().map( db -> (DatabaseMeta) db.clone() ).collect( Collectors.toList() );
  }

  @Override
  public DatabaseMeta getDatabase( String name ) throws KettleException {
    populateDbMetaMap();
    DatabaseMeta db = dbMetas.get( name );
    return db == null ? db : (DatabaseMeta) db.clone();
  }

  private void populateDbMetaMap() throws KettleException {
    if ( !initialized ) {
      synchronized( this ) {
        if ( !initialized ) {
          Map<String, Node> nodeMap = sharedObjectsIO.getSharedObjects( DB_TYPE );
          Map<String, DatabaseMeta> metaMap = new HashMap<>();
          for ( String name : nodeMap.keySet() ) {
            DatabaseMeta dbMeta = new DatabaseMeta( nodeMap.get( name ) );
            if ( !metaMap.containsKey( name ) ) {
              metaMap.put( name, dbMeta );
            }
          }
          this.dbMetas = metaMap;
        }
      }
      initialized = true;
    }

  }

  public static Node toNode( DatabaseMeta databaseMeta ) throws KettleException {
    String xml = databaseMeta.getXML();
    Document doc = XMLHandler.loadXMLString( xml );
    Node node = XMLHandler.getSubNode( doc, DB_TYPE );
    return node;
  }

  /**
   * {@inheritDoc}
   *
   * The new connection is added to xml file and also in the in memory map.
   * @param databaseMeta
   * @throws KettleException
   */
  @Override
  public synchronized void addDatabase( DatabaseMeta databaseMeta ) throws KettleException {
    populateDbMetaMap();
    String connName = databaseMeta.getName();
    // Save the database connection in xml
    Node node = toNode( databaseMeta );
    this.sharedObjectsIO.saveSharedObject( DB_TYPE, connName, node );

    // Add it to the map
    dbMetas.put( connName, (DatabaseMeta) databaseMeta.clone() );
  }

  @Override
  public synchronized void removeDatabase( DatabaseMeta databaseMeta ) throws KettleException {
    removeDatabase( databaseMeta.getName() );
  }

  @Override
  public synchronized void removeDatabase( String databaseName ) throws KettleException {
    populateDbMetaMap();

    this.sharedObjectsIO.delete( DB_TYPE, databaseName );

    dbMetas.remove( databaseName );
  }

  @Override
  public synchronized void clear() throws KettleException {
    this.sharedObjectsIO.clear( DB_TYPE );
    dbMetas.clear();
  }

  /**
   * Factory for the DatabaseConnectionManager. This factory class is registered with BowlFactory registry
   * during the initialization in KettleEnvironment
   */
  public static class DbConnectionManagerFactory implements ManagerFactory<DatabaseManagementInterface> {
    public DatabaseManagementInterface apply( Bowl bowl ) throws KettleException {
      return DatabaseConnectionManager.getInstance( bowl );
    }
  }

}