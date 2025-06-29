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


package org.pentaho.di.ui.job.entries.missing;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.missing.MissingEntry;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonPluginType;

import java.util.List;

public class MissingEntryDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = MissingEntryDialog.class;

  private Shell shell;
  private Shell shellParent;
  private List<MissingEntry> missingEntries;
  private int mode;
  private PropsUI props;
  private JobEntryInterface jobEntryResult;

  public static final int MISSING_JOB_ENTRIES = 1;
  public static final int MISSING_JOB_ENTRY_ID = 2;

  public MissingEntryDialog( Shell parent, List<MissingEntry> missingEntries ) {
    super( parent, null, null, null );
    this.shellParent = parent;
    this.missingEntries = missingEntries;
    this.mode = MISSING_JOB_ENTRIES;
  }

  public MissingEntryDialog( Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntryInt, rep, jobMeta );
    this.shellParent = parent;
    this.mode = MISSING_JOB_ENTRY_ID;
  }

  private String getErrorMessage( List<MissingEntry> missingEntries, int mode ) {
    String message = "";
    boolean hasPluginManager = hasPluginManager();

    if ( mode == MISSING_JOB_ENTRIES ) {
      StringBuilder entries = new StringBuilder();
      for ( MissingEntry entry : missingEntries ) {
        if ( missingEntries.indexOf( entry ) == missingEntries.size() - 1 ) {
          entries.append( "- " + entry.getName() + " - " + entry.getMissingPluginId() + "\n\n" );
        } else {
          entries.append( "- " + entry.getName() + " - " + entry.getMissingPluginId() + "\n" );
        }
      }
      message = BaseMessages.getString( PKG, "MissingEntryDialog.MissingJobEntries", entries.toString(),
        (hasPluginManager ? " " + BaseMessages.getString( PKG, "MissingEntryDialog.PluginManagerMessage") : "." ) );
    }

    if ( mode == MISSING_JOB_ENTRY_ID ) {
      message =
          BaseMessages.getString( PKG, "MissingEntryDialog.MissingJobEntryId",
            jobEntryInt.getName() + " - " + ( (MissingEntry) jobEntryInt ).getMissingPluginId(),
            (hasPluginManager ? " " + BaseMessages.getString( PKG, "MissingEntryDialog.PluginManagerMessage") : "." ) );
    }

    return message;
  }

  // A bit of a nasty hack... Plugin Manager is itself a plugin, and one that is not currently distributed with CE.
  // We check for the presence of a plugin with this ID to determine whether this dialog should have a button to
  // open the plugin manager interface.
  private boolean hasPluginManager() {
    PluginInterface pluginManagerPlugin = PluginRegistry.getInstance().findPluginWithId( SpoonPluginType.class, "plugin-manager-di" );
    return pluginManagerPlugin != null;
  }

  public JobEntryInterface open() {
    this.props = PropsUI.getInstance();
    Display display = shellParent.getDisplay();
    int margin = Const.MARGIN;

    shell =
        new Shell( shellParent, SWT.DIALOG_TRIM | SWT.CLOSE | SWT.ICON
            | SWT.APPLICATION_MODAL );

    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSpoon() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginLeft = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "MissingEntryDialog.MissingPlugins" ) );
    shell.setLayout( formLayout );

    Label image = new Label( shell, SWT.NONE );
    props.setLook( image );
    Image icon = display.getSystemImage( SWT.ICON_QUESTION );
    image.setImage( icon );
    FormData imageData = new FormData();
    imageData.left = new FormAttachment( 0, 5 );
    imageData.right = new FormAttachment( 11, 0 );
    imageData.top = new FormAttachment( 0, 10 );
    image.setLayoutData( imageData );

    Label error = new Label( shell, SWT.WRAP );
    props.setLook( error );
    error.setText( getErrorMessage( missingEntries, mode ) );
    FormData errorData = new FormData();
    errorData.left = new FormAttachment( image, 5 );
    errorData.right = new FormAttachment( 100, -5 );
    errorData.top = new FormAttachment( 0, 10 );
    error.setLayoutData( errorData );

    Label separator = new Label( shell, SWT.WRAP );
    props.setLook( separator );
    FormData separatorData = new FormData();
    separatorData.top = new FormAttachment( error, 10 );
    separator.setLayoutData( separatorData );

    Button closeButton = new Button( shell, SWT.PUSH );
    props.setLook( closeButton );
    FormData fdClose = new FormData();
    fdClose.right = new FormAttachment( 98 );
    fdClose.top = new FormAttachment( separator );
    closeButton.setLayoutData( fdClose );
    closeButton.setText( BaseMessages.getString( PKG, "MissingEntryDialog.Close" ) );
    closeButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        shell.dispose();
        jobEntryResult = null;
      }
    } );

    FormData fdSearch = new FormData();
    if ( this.mode == MISSING_JOB_ENTRIES ) {
      Button openButton = new Button( shell, SWT.PUSH );
      props.setLook( openButton );
      FormData fdOpen = new FormData();
      fdOpen.right = new FormAttachment( closeButton, -5 );
      fdOpen.bottom = new FormAttachment( closeButton, 0, SWT.BOTTOM );
      openButton.setLayoutData( fdOpen );
      openButton.setText( BaseMessages.getString( PKG, "MissingEntryDialog.OpenFile" ) );
      openButton.addSelectionListener( new SelectionAdapter() {
        public void widgetSelected( SelectionEvent e ) {
          shell.dispose();
          jobEntryResult = new MissingEntry();
        }
      } );
      fdSearch.right = new FormAttachment( openButton, -5 );
      fdSearch.bottom = new FormAttachment( openButton, 0, SWT.BOTTOM );
    } else {
      fdSearch.right = new FormAttachment( closeButton, -5 );
      fdSearch.bottom = new FormAttachment( closeButton, 0, SWT.BOTTOM );
    }

    if ( hasPluginManager() ) {
      Button searchButton = new Button( shell, SWT.PUSH );
      props.setLook( searchButton );
      searchButton.setText( BaseMessages.getString( PKG, "MissingEntryDialog.OpenPluginManager" ) );
      searchButton.setLayoutData( fdSearch );
      searchButton.addSelectionListener( new SelectionAdapter() {
        public void widgetSelected( SelectionEvent e ) {
          try {
            shell.dispose();
            Spoon.getInstance().openMarketplace();
          } catch ( Exception ex ) {
            ex.printStackTrace();
          }
        }
      } );
    }

    shell.pack();
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntryResult;
  }
}
