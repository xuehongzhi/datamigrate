
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import cif.base.Global;
import cif.cifplus.CifPlusUtilities;
import cif.cifplus.io.CifCategory;
import cif.dataengine.DataEngine;
import cif.dataengine.DataFormatAssistance;
import cif.dataengine.io.CategoryProperties;
import cif.dataengine.io.LogCurve1D;
import cif.dataengine.io.LogDataSource;
import cif.dataengine.io.LoggingProperties;
import cif.dataengine.io.TableFields;
import cif.dataengine.io.TableRecords;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import org.openide.util.Exceptions;
import sun.security.krb5.internal.crypto.crc32;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author xue
 */
public class DataAdaptor {

    private static DataAdaptor instance;

    public static DataAdaptor getDefault() {
        if (instance == null) {
            instance = new DataAdaptor();
        }
        return instance;
    }

    public void transform(String path) {
        DataEngine engine = DataEngine.getDefault();
        LogDataSource root = engine.getDataSource(path);

        try {

            DataFormatAssistance assistance = (DataFormatAssistance) engine.getDataFormatAssistances().toArray()[0];

            if (assistance != null && root == null) {
                root = assistance.buildDataSource();
                root.createDataSource(path);
                root.connect(path);
                DataEngine.getDefault().addDataSource(root);
            }
            if (root == null) {
                return;
            }

            root.getLogWorkSpaceList().forEach(ws -> {
                ws.getLogWellList().forEach(well -> {
                    well.getLogCategoryList().forEach(cat -> tranform(path, (CifCategory) cat));
                });
            });
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void writeAttribute(CategoryProperties props, IHDF5Writer writer, String path) {
        //  get values types unit name etc
        // need to convert from properties to tablerecords
        TableRecords recs = CifPlusUtilities.getInstance().getTableRecordsFromCategoryProperties(props);
        TableFields fields = recs.getTableFields();
        for (int k = 0; k < recs.getRecordsNum(); ++k) {
            for (int i = 0; i < fields.getFieldNum(); i++) {
                if (fields.getDataType(i) == Global.DATA_STRING) {
                    writer.string().setArrayAttr(path, fields.getName(i), recs.getRecordString(k, i));
                }
            }
        }
    }

    private void writeAttribute(LoggingProperties props, IHDF5Writer writer, String path) {
        writer.float64().setAttr(path, "startDepth", props.getStartDepth());
        writer.float64().setAttr(path, "endDepth", props.getEndDepth());
        writer.float64().setAttr(path, "depthLevel", props.getDepthLevel());
        writer.float64().setAttr(path, "dataType", props.getDataType());

    }

    public void tranform(String projPath, CifCategory category) {
        String filePath = category.getCifPlus().getCifPlusFileName();

        try {
            //save hdf file to a english direcotry
            // use base64 to encode absolute path to avoid non english directory
            Files.createDirectories(Paths.get(projPath, "cgd"));
            Path p = Paths.get(projPath, "cgd", Base64.getEncoder().encode(filePath.getBytes()).toString() + ".cgd");

            IHDF5Writer writer = HDF5Factory.open(p.toString());

            //write category attribute
            writeAttribute(category.getCategoryProperties(), writer, "/");

            //write 1d log curve
            for (int i = 0; i < category.getLogCurve1DCount(); ++i) {
                LogCurve1D curve1D = category.getLogCurve1D(i);
                LoggingProperties props = curve1D.getLoggingProperties();
                switch (curve1D.getDataType()) {
                    case Global.DATA_FLOAT:
                        float[] flts = new float[curve1D.getDepthSampleCount()];
                        curve1D.readData(props.getStartDepth(), curve1D.getDepthSampleCount(), flts, null);
                        writer.float32().writeArray(curve1D.getName(), flts);
                    default:
                        break;
                }
                //write 1d curve attribute
                writeAttribute(props, writer, curve1D.getName());

            }

            writer.close();

            //create a link in the original direcotry to the real hdf5 file
            
            Files.createLink(Paths.get(filePath.replace(".cifp", ".cgd")), p);
        } catch (HDF5Exception e) {
            e.printStackTrace();
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        }
    }

    public static void main(String[] args) {
        try {
            if (args.length != 1) {
                return;
            }

            DataAdaptor.getDefault().transform(args[0]);
        } catch (Exception e) {
        }

    }
}
