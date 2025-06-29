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


package org.pentaho.di.trans.steps.edi2xml;

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.bowl.Bowl;

@Step( id = "TypeExitEdi2XmlStep", name = "BaseStep.TypeLongDesc.Edi2Xml",
        description = "BaseStep.TypeTooltipDesc.Edi2Xml",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Utility",
        image = "EDI2XML.svg",
        documentationUrl = "http://wiki.pentaho.com/display/EAI/Edi+to+XML",
        i18nPackageName = "org.pentaho.di.trans.steps.edi2xml" )
public class Edi2XmlMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = Edi2XmlMeta.class; // for i18n purposes

  private String outputField;
  private String inputField;

  public Edi2XmlMeta() {
    super();
  }

  public String getInputField() {
    return inputField;
  }

  public void setInputField( String inputField ) {
    this.inputField = inputField;
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField( String outputField ) {
    this.outputField = outputField;
  }

  @Override
  public String getXML() throws KettleValueException {
    StringBuilder retval = new StringBuilder();

    retval.append( "   " + XMLHandler.addTagValue( "inputfield", inputField ) );
    retval.append( "   " + XMLHandler.addTagValue( "outputfield", outputField ) );

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {

    try {
      setInputField( XMLHandler.getNodeValue( XMLHandler.getSubNode( stepnode, "inputfield" ) ) );
      setOutputField( XMLHandler.getNodeValue( XMLHandler.getSubNode( stepnode, "outputfield" ) ) );
    } catch ( Exception e ) {
      throw new KettleXMLException( "Template Plugin Unable to read step info from XML node", e );
    }

  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    try {
      inputField = rep.getStepAttributeString( id_step, "inputfield" );
      outputField = rep.getStepAttributeString( id_step, "outputfield" );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages
        .getString( PKG, "Edi2Xml.Exception.UnexpectedErrorInReadingStepInfo" ), e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "inputfield", inputField );
      rep.saveStepAttribute( id_transformation, id_step, "outputfield", outputField );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages
        .getString( PKG, "Edi2Xml.Exception.UnableToSaveStepInfoToRepository" )
        + id_step, e );
    }
  }

  @Override
  public void getFields( Bowl bowl, RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, Repository repository, IMetaStore metaStore ) {

    ValueMetaInterface extra = null;

    if ( !Utils.isEmpty( getOutputField() ) ) {
      extra = new ValueMetaString( space.environmentSubstitute( getOutputField() ) );
      extra.setOrigin( origin );
      r.addValueMeta( extra );
    } else {
      if ( !Utils.isEmpty( getInputField() ) ) {
        extra = r.searchValueMeta( space.environmentSubstitute( getInputField() ) );
      }
    }

    if ( extra != null ) {
      extra.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
    }

  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
    CheckResult cr;

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is receiving input from other steps.", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta );
      remarks.add( cr );
    }

    // is the input field there?
    String realInputField = transMeta.environmentSubstitute( getInputField() );
    if ( prev.searchValueMeta( realInputField ) != null ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is seeing input field: " + realInputField, stepMeta );
      remarks.add( cr );

      if ( prev.searchValueMeta( realInputField ).isString() ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, "Field " + realInputField + " is a string type", stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult(
            CheckResult.TYPE_RESULT_OK, "Field " + realInputField + " is not a string type!", stepMeta );
        remarks.add( cr );
      }

    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, "Step is not seeing input field: "
          + realInputField + "!", stepMeta );
      remarks.add( cr );
    }

  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    outputField = "edi_xml";
    inputField = "";
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans disp ) {
    return new Edi2Xml( stepMeta, stepDataInterface, cnr, transMeta, disp );
  }

  @Override
  public StepDataInterface getStepData() {
    return new Edi2XmlData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

}
