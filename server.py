from flask import Flask, request, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Episode  # Import your Episode model

app = Flask(__name__)

# Set up database connection
DATABASE_URI = 'mysql://con:root@localhost/bobross'  # Update this URI
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

@app.route('/episodes', methods=['GET'])
def get_episodes():
    # Extract filter criteria from query parameters
    month = request.args.get('month')
    subjects = request.args.getlist('subject')
    colors = request.args.getlist('color')
    filter_type = request.args.get('filter_type', 'and')

    # Construct base query
    query = session.query(Episode)

    # Apply filters based on criteria
    if month:
        query = query.filter(Episode.air_date.like(f'%{month}%'))

    if subjects:
        query = query.filter(Episode.subject.in_(subjects))

    if colors:
        query = query.filter(Episode.color.in_(colors))

    # Execute the query
    episodes = query.all()

    # Return JSON response
    response_data = [{'title': episode.title, 'air_date': episode.air_date, ...} for episode in episodes]
    return jsonify(response_data)

if __name__ == '__main__':
    app.run(debug=True)
