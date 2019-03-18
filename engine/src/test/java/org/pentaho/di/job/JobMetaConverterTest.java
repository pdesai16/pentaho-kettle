/*
 * ! ******************************************************************************
 *
 *  Pentaho Data Integration
 *
 *  Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 * ******************************************************************************
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * *****************************************************************************
 */
package org.pentaho.di.job;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.engine.api.model.Transformation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.HashMap;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JobMetaConverterTest {

  @BeforeClass
  public static void init() throws Exception {
    KettleEnvironment.init();
    PluginRegistry.addPluginType( StepPluginType.getInstance() );
    PluginRegistry.init();
    if ( !Props.isInitialized() ) {
      Props.init( 0 );
    }
  }

  @AfterClass
  public static void cleanUp() {
    KettleClientEnvironment.reset();
  }

  @Test
  public void simpleConvert() throws Exception {
    JobMeta jobMeta = new JobMeta( getClass().getResource( "Process_changelog.kjb" ).getPath(), null );
    Transformation trans = JobMetaConverter.convert( jobMeta );
    assertThat( trans.getId(), is( jobMeta.getFilename() ) );
  }

  /*
  Set arguments on a transformation
   */
  @Test
  public void testIncludesSubTransformations() throws Exception {
    JobMeta parentJobMeta = new JobMeta( getClass().getResource( "Set arguments on a transformation.kjb" ).getPath(), null, null );
    Transformation transformation = JobMetaConverter.convert( parentJobMeta );

    //TEST Code
    Optional<String> ops = convertToString( transformation );
    System.out.println( ops.toString() );

    @SuppressWarnings( { "unchecked", "ConstantConditions" } )
    HashMap<String, Transformation> config =
      (HashMap<String, Transformation>) transformation.getConfig( JobMetaConverter.SUB_TRANSFORMATIONS_KEY ).get();
    assertEquals( 1, config.size() );
    //assertNotNull( config.get( "file://" + getClass().getResource( "Run transformation.ktr" ).getPath() ) );
  }

  /**
   * Serialize the object to Base64 encoded string
   * @param object
   * @return
   */
  static Optional<String> convertToString( final Serializable object) {
    try ( final ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(object);
      return Optional.of( Base64.getEncoder().encodeToString(baos.toByteArray()));
    } catch (final IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }
}
