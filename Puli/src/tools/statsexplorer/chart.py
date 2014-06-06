from pygooglechart import PieChart3D

# Create a chart object of 250x100 pixels
chart = PieChart3D(250, 100)

# Add some data
chart.add_data([20, 10,10,1,2,5,4,8,6,2,8,41,5,4,7,9])

# Assign the labels to the pie data
# chart.set_pie_labels(['Hello', 'World'])

# Print the chart URL
# print chart.get_url()

# Download the chart
chart.download('pie-hello-world.png')