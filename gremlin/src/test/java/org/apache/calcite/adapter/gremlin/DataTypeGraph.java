package org.apache.calcite.adapter.gremlin;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static org.apache.tinkerpop.gremlin.structure.T.label;

public class DataTypeGraph implements TestGraph {
    @Override
    public void populate(final Graph graph) {
        // Create vertices for each data type.
        final Vertex stringtype = graph.addVertex(label, "stringtype", "key", GraphConstants.STRING_VALUE);
        final Vertex bytetype = graph.addVertex(label, "bytetype", "key", GraphConstants.BYTE_VALUE);
        final Vertex shorttype = graph.addVertex(label, "shorttype", "key", GraphConstants.SHORT_VALUE);
        final Vertex inttype = graph.addVertex(label, "inttype", "key", GraphConstants.INTEGER_VALUE);
        final Vertex longtype = graph.addVertex(label, "longtype", "key", GraphConstants.LONG_VALUE);
        final Vertex floattype = graph.addVertex(label, "floattype", "key", GraphConstants.FLOAT_VALUE);
        final Vertex doubletype = graph.addVertex(label, "doubletype", "key", GraphConstants.DOUBLE_VALUE);
        final Vertex datetype = graph.addVertex(label, "datetype", "key", GraphConstants.DATE_VALUE);

        // Create edge from vertices to themselves with data types so they can be tested.
        stringtype.addEdge("stringtypeedge", stringtype, "key", GraphConstants.STRING_VALUE);
        bytetype.addEdge("bytetypeedge", bytetype, "key", GraphConstants.BYTE_VALUE);
        shorttype.addEdge("shorttypeedge", shorttype, "key", GraphConstants.SHORT_VALUE);
        inttype.addEdge("inttypeedge", inttype, "key", GraphConstants.INTEGER_VALUE);
        longtype.addEdge("longtypeedge", longtype, "key", GraphConstants.LONG_VALUE);
        floattype.addEdge("floattypeedge", floattype, "key", GraphConstants.FLOAT_VALUE);
        doubletype.addEdge("doubletypeedge", doubletype, "key", GraphConstants.DOUBLE_VALUE);
        datetype.addEdge("datetypeedge", datetype, "key", GraphConstants.DATE_VALUE);
    }
}
