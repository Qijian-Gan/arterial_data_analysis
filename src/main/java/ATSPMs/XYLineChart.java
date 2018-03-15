package ATSPMs;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.sql.ResultSet;


public class XYLineChart extends ApplicationFrame {
    // A demonstration application showing an XY series containing a null value.
    // @param title  the frame title.
    public XYLineChart(final String title,final String xLabel, final String yLabel, final XYSeries series) {
        super(title);

        final XYSeriesCollection data = new XYSeriesCollection(series);
        final JFreeChart chart = ChartFactory.createXYLineChart(title,xLabel,yLabel,
                data,PlotOrientation.VERTICAL,true,true,false);

        final ChartPanel chartPanel = new ChartPanel(chart);
        setContentPane(chartPanel);
        ChartFactory.setChartTheme(StandardChartTheme.createJFreeTheme());
        chartPanel.setPreferredSize(new java.awt.Dimension(1000, 500));
        final ApplicationFrame frame = new ApplicationFrame(title);
        frame.setContentPane(chartPanel);
        frame.pack();
        frame.setVisible(true);
    }
}
