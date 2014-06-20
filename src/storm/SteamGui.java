package storm;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.sql.Date;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;


public class SteamGui extends JFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	TimeSeries online;
	ChartPanel chartPanel;
	JPanel chartPane;
	JPanel iterationCounterPane;
	JPanel progressBarPane;
	JPanel queryCounterPane;
	JPanel invalidIdPane;
	JProgressBar progressBar;
	JTextField invalidCounter;
	JTextArea invalidList;
//	private ArrayList<Long[]> history;
	
	public SteamGui(){
		setTitle("Steam GUI");
		setSize(1100,500);
		online = new TimeSeries("Users Online");
		chartPane = new JPanel();
		iterationCounterPane = new JPanel();
		progressBarPane = new JPanel();
		queryCounterPane = new JPanel();
		invalidIdPane = new JPanel();
		setLayout(new GridBagLayout());
		GridBagConstraints constraints = new GridBagConstraints();
		
		constraints.gridwidth = 10;
		constraints.gridheight = 10;
		constraints.gridx = 0;
		constraints.gridy = 0;
		constraints.weightx = 0.5;
		constraints.weighty = 0.5;
		//constraints.fill = GridBagConstraints.HORIZONTAL;
		add(chartPane, constraints);
		
		constraints.gridx = 10;
		constraints.gridy = 0;
		constraints.gridwidth = 2;
		constraints.gridheight = 2;
		add(invalidIdPane, constraints);
		createInvalidPane();
		
		constraints.gridx = 0;
		constraints.gridy = 12;
		constraints.gridwidth = 1;
		constraints.gridheight = 1;
		constraints.weightx = 0.5;
		add(iterationCounterPane, constraints);
		setIteration(0);
		
		constraints.gridx = 6;
		constraints.gridy = 12;
		constraints.gridwidth = 1;
		constraints.gridheight = 1;
		constraints.weightx = 0.5;
		add(queryCounterPane, constraints);
		updateQueryPane(0);
		
		constraints.gridx = 10;
		constraints.gridy = 12;
		constraints.gridwidth = 1;
		constraints.gridheight = 1;
		constraints.weightx = 0.5;
		add(progressBarPane, constraints);
		
		progressBar = new JProgressBar(0, 100);
		progressBar.setValue(0);
		progressBar.setStringPainted(true);
		
		JLabel label = new JLabel("Current iteration progress:");
		progressBarPane.add(label);
		progressBarPane.add(progressBar);
		
		setDefaultCloseOperation(EXIT_ON_CLOSE);
		setVisible(true);
	}
	
	public void updateProgressBar(double percentage){
		progressBar.setValue((int) percentage);
	}
	
	public void addDataPoint(Long time, int numOnline){
		Date date = new Date(time);
		online.add(new Millisecond(date), numOnline);
		JFreeChart chart = createChart();
		chartPanel = new ChartPanel(chart);
//		chartPanel.setPreferredSize(new java.awt.Dimension(900, 400));
		chartPane.removeAll();
		chartPane.add(chartPanel);
//		setContentPane(chartPanel);
		revalidate();
		repaint();
	}
	
	public void setIteration(int iteration){
		JLabel label = new JLabel("Current iteration:");
		JTextField iterationField = new JTextField(5);
		iterationField.setText(""+iteration);
		iterationField.setEditable(false);
		iterationCounterPane.removeAll();
		iterationCounterPane.add(label);
		iterationCounterPane.add(iterationField);
		revalidate();
		repaint();
	}

	private JFreeChart createChart() {
		TimeSeriesCollection dataset = new TimeSeriesCollection(online);
		
		JFreeChart chart = ChartFactory.createTimeSeriesChart("Users Online", "Time", "Users Online", dataset);
		
		return chart;
	}
	
	public void updateQueryPane(int numQueries){
		JLabel label = new JLabel("Number of Queries: ");
		JTextField queriesField = new JTextField(5);
		queriesField.setText(""+numQueries);
		queriesField.setEditable(false);
		queryCounterPane.removeAll();
		queryCounterPane.add(label);
		queryCounterPane.add(queriesField);
		revalidate();
		repaint();
	}
	
	public void createInvalidPane(){
		invalidIdPane.setLayout(new GridBagLayout());
		GridBagConstraints constraints = new GridBagConstraints();
		constraints.gridx = 0;
		constraints.gridy = 0;
		constraints.gridwidth = 1;
		constraints.gridheight = 1;
		constraints.weighty = 0.5;
		
		JLabel label = new JLabel("Invalid ID count: ");
		invalidIdPane.add(label, constraints);
		
		constraints.gridx = 1;
		constraints.gridy = 0;
		
		invalidCounter = new JTextField(5);
		invalidCounter.setEditable(false);
		invalidIdPane.add(invalidCounter, constraints);
		
		constraints.gridx = 0;
		constraints.gridy = 2;
		constraints.gridwidth = 5;
		constraints.gridheight = 3;
		
		invalidList = new JTextArea(25, 35);
		invalidList.setEditable(false);
		JScrollPane scrollPane = new JScrollPane(invalidList, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		invalidIdPane.add(scrollPane, constraints);
		revalidate();
		repaint();
	}
	
	public void updateInvalidPane(int numInvalid, String invalidID){
		invalidCounter.setText(""+numInvalid);
		String previousText = invalidList.getText();
		String newText = invalidID + "\n" + previousText;
		invalidList.setText(newText);
		revalidate();
		repaint();
	}
	
	
	public static void main(String[] args) {
	    SteamGui f = new SteamGui();
	    f.addDataPoint(System.currentTimeMillis(), 50);try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    f.addDataPoint(System.currentTimeMillis(), 100);
	    f.addDataPoint(System.currentTimeMillis(), 50);try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    f.addDataPoint(System.currentTimeMillis(), 30);
	    f.addDataPoint(System.currentTimeMillis(), 50);try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    f.addDataPoint(System.currentTimeMillis(), 10);
	    f.setIteration(5);
	    for(int i = 0; i<=100; i++){
	    	try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	f.updateProgressBar(i);
	    	f.updateQueryPane(i);
	    	f.addDataPoint(System.currentTimeMillis(), (int) (Math.random()*200));
	    	f.updateInvalidPane(i, new Double(Math.random()).toString());
	    	f.setIteration(i);
	    }
	}
}
