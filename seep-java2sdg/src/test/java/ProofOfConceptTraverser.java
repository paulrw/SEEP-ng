import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;
import org.codehaus.janino.Java.Annotation;
import org.codehaus.janino.Java.LocalVariable;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.util.Traverser;


public class ProofOfConceptTraverser extends Traverser {

    public static void main(String[] args) throws CompileException, IOException {
    	ProofOfConceptTraverser poct = new ProofOfConceptTraverser();
        for (String fileName : args) {

            // Parse each compilation unit.
            FileReader           r = new FileReader(fileName);
            Java.CompilationUnit cu;
            try {
                cu = new Parser(new Scanner(fileName, r)).parseCompilationUnit();
            } finally {
                r.close();
            }

            // Traverse it and count declarations.
            poct.traverseCompilationUnit(cu);
        }

        System.out.println("Class declarations:     " + poct.classDeclarationCount);
        System.out.println("Interface declarations: " + poct.interfaceDeclarationCount);
        System.out.println("Fields:                 " + poct.fieldCount);
        System.out.println("Local variables:        " + poct.localVariableCount);
        System.out.println("Annotations: "+poct.annotationCount);
        System.out.println("Annotations2: "+poct.annotationCount2);
        System.out.println("Annotations3: "+poct.annotationCount3);
        System.out.println("Annotations4: "+poct.annotationCount4);
    }
    
    // Count annotations
    @Override public void traverseAnnotation(Java.Annotation an){
    	++this.annotationCount;
    	System.out.println("type: "+an.getType().toString());
    	super.traverseAnnotation(an);
    }
    private int annotationCount;
    
 // Count annotations
    @Override public void traverseMarkerAnnotation(Java.MarkerAnnotation an){
    	++this.annotationCount2;
    	System.out.println("type: "+an.getType().toString());
    	super.traverseAnnotation(an);
    }
    private int annotationCount2;
    
 // Count annotations
    @Override public void traverseNormalAnnotation(Java.NormalAnnotation an){
    	++this.annotationCount3;
    	System.out.println("type: "+an.getType().toString());
    	super.traverseAnnotation(an);
    }
    private int annotationCount3;
    
 // Count annotations
    @Override public void traverseSingleElementAnnotation(Java.SingleElementAnnotation an){
    	++this.annotationCount4;
    	System.out.println("type: "+an.getType().toString());
    	super.traverseAnnotation(an);
    }
    private int annotationCount4;

    // Count class declarations.
    @Override public void traverseClassDeclaration(Java.ClassDeclaration cd) {
        ++this.classDeclarationCount;
        super.traverseClassDeclaration(cd);
    }
    private int classDeclarationCount;

    // Count interface declarations.
    @Override public void
    traverseInterfaceDeclaration(Java.InterfaceDeclaration id) {
        ++this.interfaceDeclarationCount;
        super.traverseInterfaceDeclaration(id);
    }
    private int interfaceDeclarationCount;

    // Count fields.
    @Override public void
    traverseFieldDeclaration(Java.FieldDeclaration fd) {
        this.fieldCount += fd.variableDeclarators.length;
        Annotation[] annotations = fd.getAnnotations();
        for(Annotation ann : annotations){
        	System.out.println("annotation: "+ann.toString());
        }
        System.out.println("Field: "+fd.type.toString());
        super.traverseFieldDeclaration(fd);
    }
    private int fieldCount;

    // Count local variables.
    @Override public void
    traverseLocalVariableDeclarationStatement(Java.LocalVariableDeclarationStatement lvds) {
        this.localVariableCount += lvds.variableDeclarators.length;
        System.out.println("lineNumber: "+lvds.getLocation().getLineNumber()+ " type: "+lvds.type.toString());
        
        if(lvds.localVariables != null){
        	for(Entry<String, LocalVariable> entry : lvds.localVariables.entrySet()){
        		System.out.println("name: "+entry.getKey());
        		System.out.println("value: "+entry.getValue());
        	}
        }
        super.traverseLocalVariableDeclarationStatement(lvds);
    }
    private int localVariableCount;
}
