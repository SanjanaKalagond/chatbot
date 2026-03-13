from app.router.query_router import route_query

def chat(question):
    routed = route_query(question)
    return str(routed["data"])