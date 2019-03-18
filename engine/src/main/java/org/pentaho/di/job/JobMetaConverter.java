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

import com.google.common.base.Throwables;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.engine.api.model.Transformation;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.resource.ResourceEntry;
import org.pentaho.di.trans.TransMeta;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toMap;

public class JobMetaConverter {

  public static final String JOB_META_CONF_KEY = "JobsMeta";
  public static final String JOB_META_NAME_CONF_KEY = "JobsMetaName";
  public static final String SUB_TRANSFORMATIONS_KEY = "SubTransformations";
  public static final String STEP_META_CONF_KEY = "StepMeta";
  public static final String JOB_DEFAULT_NAME = "No Name";
  public static List<String> JOB_ENTRIES = new ArrayList<String>( Arrays.asList("START","DUMMY","OK","ERROR" ));

  public static Transformation convert(JobMeta jobMeta ) {
    final org.pentaho.di.engine.model.Transformation transformation =
      new org.pentaho.di.engine.model.Transformation( createJobId( jobMeta ) );
    try {
      JobMeta copyJobMeta = (JobMeta) jobMeta.realClone( false );
      //cleanupDisabledHops( copyJobMeta );
      VariableSpace var = new Variables();
      copyJobMeta.setInternalKettleVariables( var );
      transformation.setConfig( JOB_META_CONF_KEY, copyJobMeta.getXML() );
      transformation.setConfig( JOB_META_NAME_CONF_KEY,
        Optional.ofNullable( jobMeta.getName() ).orElse( JOB_DEFAULT_NAME ) );

       Map<String, Transformation> subTransformations = copyJobMeta.getResourceDependencies().stream()
        .flatMap( resourceReference -> resourceReference.getEntries().stream() )
        .filter( entry -> ResourceEntry.ResourceType.ACTIONFILE.equals( entry.getResourcetype() ) )
        .collect( toMap( ResourceEntry::getResource, entry -> {
          try {
            Repository repository = copyJobMeta.getRepository();
            if ( repository != null ) {
              Path path = Paths.get( entry.getResource() );
              RepositoryDirectoryInterface directory =
                repository.findDirectory( path.getParent().toString().replace( File.separator, "/" ) );
              return convert(
                repository.loadJob( path.getFileName().toString(), directory, null, null ) );
            }
            return convert( new JobMeta( copyJobMeta.getParentVariableSpace(), entry.getResource(), null, null ) );
          } catch ( KettleException e ) {
            throw new RuntimeException( e );
          }
        } ) );
      transformation.setConfig( SUB_TRANSFORMATIONS_KEY, (Serializable) subTransformations );
    } catch ( Exception e ) {
      Throwables.propagate( e );
    }
    return transformation;
  }

  private static String createJobId( JobMeta jobMeta ) {
    String filename = jobMeta.getFilename();
    if ( !Utils.isEmpty( filename ) ) {
      return filename;
    }

    return getPathAndName( jobMeta );
  }

  public static String getPathAndName( JobMeta jobMeta ) {
    if ( jobMeta.getRepositoryDirectory().isRoot() ) {
      return jobMeta.getRepositoryDirectory().getPath() + jobMeta.getName();
    } else {
      return jobMeta.getRepositoryDirectory().getPath() + RepositoryDirectory.DIRECTORY_SEPARATOR + jobMeta.getName();
    }
  }

  private static List<JobHopMeta> findHops( JobMeta jobMeta, Predicate<JobHopMeta> condition ) {
    return IntStream.range( 0, jobMeta.nrJobHops() ).mapToObj( jobMeta::getJobHop ).filter( condition ).collect(
      Collectors.toList() );
  }

  /**
   * Removes input steps having only disabled output hops so they will not be executed.
   * @param JobMeta JobMeta to process
   */
  private static void removeDisabledInputs( JobMeta jobMeta ) {
    List<JobEntryCopy> unusedInputs = findHops( jobMeta, hop -> !hop.isEnabled() ).stream()
      .map( hop -> hop.getFromEntry() )
      .filter( jobEntryCopy -> isUnusedInput( jobMeta, jobEntryCopy ) )
      .collect( Collectors.toList() );
    for ( JobEntryCopy unusedInput : unusedInputs ) {
      List<JobHopMeta> outHops = findAllJobHopFrom( jobMeta, unusedInput );
      List<JobEntryCopy> subsequentSteps = outHops.stream().map( hop -> hop.getToEntry() ).collect( Collectors.toList() );
      outHops.forEach( jobMeta::removeJobHop );
      jobMeta.getJobCopies().remove( unusedInput );
      //removeInactivePaths( jobMeta, subsequentSteps );
    }
  }

  private static List<JobHopMeta> findAllJobHopFrom( JobMeta jobMeta, JobEntryCopy entry ){
    return jobMeta.getJobhops().stream()
      .filter( hop ->  hop.getFromEntry() != null && hop.getFromEntry().equals( entry ) )
      .collect( Collectors.toList() );
  }

  private static boolean isUnusedInput( JobMeta Job, JobEntryCopy entry ) {
    int nrEnabledOutHops = findHops( Job, hop -> hop.getFromEntry().equals( entry ) && hop.isEnabled() ).size();
    int nrDisabledOutHops = findHops( Job, hop -> hop.getFromEntry().equals( entry ) && !hop.isEnabled() ).size();
    int nrInputHops = findHops( Job, hop -> hop.getToEntry().equals( entry ) ).size();

    return ( nrEnabledOutHops == 0 && nrDisabledOutHops > 0 && nrInputHops == 0 );
  }

}
