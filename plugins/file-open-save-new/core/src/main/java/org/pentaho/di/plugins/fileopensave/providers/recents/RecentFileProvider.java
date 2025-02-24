/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.plugins.fileopensave.providers.recents;

import org.pentaho.di.core.bowl.Bowl;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.LastUsedFile;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.plugins.fileopensave.api.overwrite.OverwriteStatus;
import org.pentaho.di.plugins.fileopensave.api.providers.BaseFileProvider;
import org.pentaho.di.plugins.fileopensave.api.providers.exception.FileException;
import org.pentaho.di.plugins.fileopensave.api.providers.File;
import org.pentaho.di.plugins.fileopensave.api.providers.Tree;
import org.pentaho.di.plugins.fileopensave.providers.recents.model.RecentFile;
import org.pentaho.di.plugins.fileopensave.providers.recents.model.RecentTree;
import org.pentaho.di.plugins.fileopensave.providers.repository.model.RepositoryFile;
import org.pentaho.di.plugins.fileopensave.providers.repository.model.RepositoryTree;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.spoon.Spoon;

import java.io.InputStream;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.function.Supplier;
import java.util.List;
import java.util.stream.Collectors;

public class RecentFileProvider extends BaseFileProvider<RecentFile> {

  public static final String TYPE = "recents";
  public static final String NAME = "Recents";

  private Supplier<PropsUI> propsUISupplier = PropsUI::getInstance;

  @Override public Class<RecentFile> getFileClass() {
    return RecentFile.class;
  }

  @Override public String getName() {
    return null;
  }

  @Override public String getType() {
    return TYPE;
  }

  @Override public boolean isAvailable() {
    return true;
  }

  @Override
  public Tree getTree( Bowl bowl ) {
    Tree recentTree;

    PropsUI propsUI = getPropsUI();
    Date dateThreshold = getDateThreshold();
    List<LastUsedFile> lastUsedFiles;
    final Spoon spoonInstance = Spoon.getInstance();
    if ( spoonInstance.rep == null ) {
      lastUsedFiles = propsUI.getLastUsedFiles().stream()
        .filter(
          lastUsedFile -> !lastUsedFile.isSourceRepository() && !lastUsedFile.getLastOpened().before( dateThreshold ) )
        .collect( Collectors.toList() );
      recentTree = new RecentTree( NAME );
      for ( LastUsedFile lastUsedFile : lastUsedFiles ) {
        recentTree.addChild( RecentFile.create( lastUsedFile ) );
      }
    } else {
      IUser userInfo = spoonInstance.rep.getUserInfo();
      String repoAndUser = spoonInstance.rep.getName() + ":" + ( userInfo != null ? userInfo.getLogin() : "" );
      lastUsedFiles = propsUI.getLastUsedRepoFiles().getOrDefault( repoAndUser, Collections.emptyList() ).stream()
        .filter( lastUsedFile -> !lastUsedFile.getLastOpened().before( dateThreshold ) ).collect( Collectors.toList() );
      recentTree = new RepositoryTree( NAME );
      getLastUsedFile( lastUsedFiles, spoonInstance, recentTree );
    }

    return recentTree;
  }

  @Override
  public List<RecentFile> getFiles( Bowl bowl, RecentFile file, String filters, VariableSpace space )
    throws FileException {
    return Collections.emptyList();
  }

  @Override
  public List<RecentFile> delete( Bowl bowl, List<RecentFile> files, VariableSpace space ) throws FileException {
    return Collections.emptyList();
  }

  @Override
  public RecentFile add( Bowl bowl, RecentFile folder, VariableSpace space ) throws FileException {
    return null;
  }

  @Override
  public RecentFile getFile( Bowl bowl, RecentFile file, VariableSpace space ) {
    return null;
  }

  @Override
  public boolean fileExists( Bowl bowl, RecentFile dir, String path, VariableSpace space ) throws FileException {
    return false;
  }

  @Override
  public String getNewName( Bowl bowl, RecentFile destDir, String newPath, VariableSpace space ) throws FileException {
    return null;
  }

  @Override
  public boolean isSame( Bowl bowl, File file1, File file2 ) {
    return false;
  }

  @Override
  public RecentFile rename( Bowl bowl, RecentFile file, String newPath, OverwriteStatus overwriteStatus,
                            VariableSpace space ) throws FileException {
    return null;
  }

  @Override
  public RecentFile copy( Bowl bowl, RecentFile file, String toPath, OverwriteStatus overwriteStatus,
                          VariableSpace space ) throws FileException {
    return null;
  }

  @Override
  public RecentFile move( Bowl bowl, RecentFile file, String toPath, OverwriteStatus overwriteStatus,
                          VariableSpace space ) throws FileException {
    return null;
  }

  @Override
  public InputStream readFile( Bowl bowl, RecentFile file, VariableSpace space ) throws FileException {
    return null;
  }

  @Override
  public RecentFile writeFile( Bowl bowl, InputStream inputStream, RecentFile destDir, String path,
                               OverwriteStatus overwriteStatus, VariableSpace space )
    throws FileException {
    return null;
  }

  @Override
  public RecentFile getParent( Bowl bowl, RecentFile file ) {
    return null;
  }

  @Override
  public void clearProviderCache() {
    // Not cached
  }

  @Override
  public RecentFile createDirectory( Bowl bowl, String parentPath, RecentFile file, String newDirectoryName ) {
    return null;
  }

  private PropsUI getPropsUI() {
    return propsUISupplier.get();
  }

  private Date getDateThreshold() {
    Calendar calendar = Calendar.getInstance();
    calendar.add( Calendar.DATE, -30 );
    return calendar.getTime();
  }

  private void getLastUsedFile( List<LastUsedFile> lastUsedFiles, Spoon spoonInstance, Tree recentTree ) {
    for ( LastUsedFile lastUsedFile : lastUsedFiles ) {
      ObjectId objectID;
      try {
        if ( lastUsedFile.isTransformation() ) {
          objectID = spoonInstance.rep.getTransformationID( lastUsedFile.getFilename(),
            spoonInstance.rep.findDirectory( lastUsedFile.getDirectory() ) );
        } else {
          objectID = spoonInstance.rep.getJobId( lastUsedFile.getFilename(),
            spoonInstance.rep.findDirectory( lastUsedFile.getDirectory() ) );
        }
      } catch ( KettleException | IllegalAccessError e ) {
        objectID = null;
      }
      if ( objectID != null ) {
        recentTree.addChild( RepositoryFile.create( lastUsedFile, objectID ) );
      }
    }
  }
}
