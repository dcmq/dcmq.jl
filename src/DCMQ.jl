module DCMQ
using AMQPClient
using DICOM

export consumer_loop, publish_nifti

function publish_nifti(channel, dcmout, uri)
    if tag"Pixel Data" in keys(dcmout)
        delete!(dcmout, tag"Pixel Data")
    end
    io = IOBuffer()
    dcm_write(io, dcmout)
    msg_out = Message(io.data, headers=Dict{String,Any}("uri" => uri))
    basic_publish(channel, msg_out; exchange="dicom", routing_key="stored.series.nii")
end

function consumer_loop(server, queue, methods, dcmhandler; port = AMQPClient.AMQP_DEFAULT_PORT)
    login = "guest"
    password = "guest"
    auth_params = Dict{String,Any}("MECHANISM"=>"AMQPLAIN", "LOGIN"=>login, "PASSWORD"=>password)

    conn = connection(;virtualhost="/", host=server, port=port, auth_params=auth_params)
    chan = channel(conn, AMQPClient.UNUSED_CHANNEL, true)
    exchange_declare(chan, "dicom", EXCHANGE_TYPE_TOPIC)
    queue_declare(chan, queue)
    for method = methods
        queue_bind(chan, queue, "dicom", method)
    end

    println("entering polling loop")
    while true
        maybe_msg = basic_get(chan, queue, false)
        # check if we got a message
        if !isnothing(maybe_msg)
            println("message received")
            msg = maybe_msg
            try
                io = IOBuffer(msg.data)
                d = dcm_parse(io)
                uri = extractHeaders(msg)["uri"]
                dcmhandler(chan, d, uri)
                basic_ack(chan, msg.delivery_tag)
            catch e
                println(e)
                basic_reject(chan, msg.delivery_tag; requeue=true)
                throw(e)
            end
        else
            sleep(1)
        end
    end

    if isopen(conn)
        close(conn)
        # close is an asynchronous operation. To wait for the negotiation to complete:
        AMQPClient.wait_for_state(conn, AMQPClient.CONN_STATE_CLOSED)
    end
end

"Return AMQP messsage headers as dictionary."
function extractHeaders(msg::AMQPClient.Message)
    d = Dict()
    for x ∈ msg.properties[:headers].data
        key = String(copy(x.name.data))
        value = String(copy(x.val.fld.data))
        d[key] = value
    end
    return d
end

end # module
