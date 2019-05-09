
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import cif.baseutil.PathUtil;
import cif.cifplus.io.CifCategory;
import cif.cifplus.io.CifCurve1D;
import cif.cifplus.io.CifDataSource;
import cif.cifplus.io.CifWell;
import cif.cifplus.io.CifWorkSpace;
import cif.dataengine.DataEngine;
import cif.dataengine.DataFormatAssistance;
import cif.dataengine.io.LogCategory;
import cif.dataengine.io.LogDataSource;
import cif.dataengine.io.LogWorkSpace;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import org.openide.util.Exceptions;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author xue
 */
public class MultiReader {
    
    LogCategory cifCategory = null;
    IHDF5Writer hdfCategory = null;
    
    private void initCifCategory(String filePath){
        Path p =  Paths.get(filePath);
        String name =  PathUtil.getBaseName( p.getFileName().toString());
        p = p.getParent();
        String well = p.getFileName().toString();
        p = p.getParent();
        String workspace = p.getFileName().toString();
        String projPath = p.getParent().toString();
        
        LogDataSource root =  DataEngine.getDefault().getDataSource(projPath);
        DataFormatAssistance assistance = (DataFormatAssistance) DataEngine.getDefault().getDataFormatAssistances().toArray()[0];

        if (assistance != null && root == null) {
            root = assistance.buildDataSource();
            root.createDataSource(projPath);
            root.connect(projPath);
            DataEngine.getDefault().addDataSource(root);
        }
        cifCategory = new CifCategory(new CifWell(new CifWorkSpace(root, workspace), well), name);
    }
    
    public void initCategory(String filePath) {
        initCifCategory(filePath);
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath.replace(".cifp", ".cgd")));
              hdfCategory = HDF5Factory.open(br.readLine());
        } catch (FileNotFoundException ex) {
           
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        }
    }
    
    public void testTime1D(String curveName) {
        if (cifCategory==null || hdfCategory == null) {
            return;
        }
        
        CifCurve1D curve1D = (CifCurve1D)cifCategory.getLogCurve1D(curveName);
        float[] vals = new float[curve1D.getDepthSampleCount()];
        long l = System.currentTimeMillis();
        curve1D.readData(curve1D.getStartDepth(), curve1D.getDepthSampleCount(), vals, null);
        System.out.println(System.currentTimeMillis()-l);
        l = System.currentTimeMillis();
        float[] vals1 = hdfCategory.float32().readArray(curveName);
        System.out.println(System.currentTimeMillis()-l);
    }
}
