extern "C" {
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
}

#include "xmstream.h"


typedef struct {
  ssize_t bodySize;
} ngx_http_cxxrest_ctx_t;
 
static ngx_str_t  ngx_http_cxxrest_hide_headers[] = {
  ngx_string("Status"),  
  ngx_null_string
};
 

typedef struct {
  ngx_http_upstream_conf_t upstream;
  u_char *destination; 
} ngx_http_cxxrest_loc_conf_t;


static inline const char *methodName(ngx_uint_t method);


static char *ngx_http_cxxrest_pass_handler(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static void *ngx_zmq_upstream_loc_conf(ngx_conf_t *cf);
static char *ngx_http_cxxrest_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);

static ngx_int_t ngx_http_cxxrest_non_buffered_filter_init(void *data);
static ngx_int_t ngx_http_cxxrest_non_buffered_filter(void *data, ssize_t bytes);

static ngx_int_t ngx_http_cxxrest_handler(ngx_http_request_t *r);
static ngx_int_t ngx_http_cxxrest_create_request(ngx_http_request_t *r);
static ngx_int_t ngx_http_cxxrest_reinit_request(ngx_http_request_t *r);
static ngx_int_t ngx_http_cxxrest_process_header(ngx_http_request_t *r);
static void ngx_http_cxxrest_abort_request(ngx_http_request_t *r);
static void ngx_http_cxxrest_finalize_request(ngx_http_request_t *r, ngx_int_t rc);

static ngx_command_t  ngx_http_cxxrest_commands[] = {
    { ngx_string("cxxrest_pass"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
                        |NGX_HTTP_LIF_CONF|NGX_CONF_TAKE1,
      ngx_http_cxxrest_pass_handler,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cxxrest_loc_conf_t, destination),
      NULL },
      
    ngx_null_command
}; 
 
static ngx_http_module_t  ngx_http_cxxrest_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,    /* postconfiguration */

    NULL,     /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_zmq_upstream_loc_conf,      /* create location configuration */
    ngx_http_cxxrest_merge_loc_conf        /* merge location configuration */
};
 
ngx_module_t  ngx_http_cxxrest_module = {
    NGX_MODULE_V1,
    &ngx_http_cxxrest_module_ctx,   /* module context */
    ngx_http_cxxrest_commands,      /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static void *ngx_zmq_upstream_loc_conf(ngx_conf_t *cf)
{
  ngx_http_cxxrest_loc_conf_t *locationConf = (ngx_http_cxxrest_loc_conf_t*)ngx_pcalloc(cf->pool, sizeof(ngx_http_cxxrest_loc_conf_t));
  if (locationConf == NULL)
      return NULL;

  locationConf->destination = 0;
  locationConf->upstream.local = (ngx_http_upstream_local_t*)NGX_CONF_UNSET_PTR;
  locationConf->upstream.next_upstream_tries = NGX_CONF_UNSET_UINT;
  locationConf->upstream.connect_timeout = NGX_CONF_UNSET_MSEC;
  locationConf->upstream.send_timeout = NGX_CONF_UNSET_MSEC;
  locationConf->upstream.read_timeout = NGX_CONF_UNSET_MSEC;
  locationConf->upstream.next_upstream_timeout = NGX_CONF_UNSET_MSEC;
  locationConf->upstream.buffer_size = NGX_CONF_UNSET_SIZE;
  locationConf->upstream.hide_headers = (ngx_array_t*)NGX_CONF_UNSET_PTR;  
  locationConf->upstream.pass_headers = (ngx_array_t*)NGX_CONF_UNSET_PTR;  

    /* the hardcoded values */
  locationConf->upstream.cyclic_temp_file = 0;
  locationConf->upstream.buffering = 0;
  locationConf->upstream.ignore_client_abort = 0;
  locationConf->upstream.send_lowat = 0;
  locationConf->upstream.bufs.num = 0;
  locationConf->upstream.busy_buffers_size = 0;
  locationConf->upstream.max_temp_file_size = 0;
  locationConf->upstream.temp_file_write_size = 0;
  locationConf->upstream.intercept_errors = 1;
  locationConf->upstream.intercept_404 = 1;
  locationConf->upstream.pass_request_headers = 0;
  locationConf->upstream.pass_request_body = 0;
  locationConf->upstream.force_ranges = 1;
  ngx_str_set(&locationConf->upstream.module, "erl");  
  
  return locationConf;
}

static char *
ngx_http_cxxrest_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
  ngx_http_cxxrest_loc_conf_t *prev = (ngx_http_cxxrest_loc_conf_t*)parent;
  ngx_http_cxxrest_loc_conf_t *conf = (ngx_http_cxxrest_loc_conf_t*)child;

  ngx_conf_merge_ptr_value(conf->upstream.local, prev->upstream.local, NULL);
  ngx_conf_merge_uint_value(conf->upstream.next_upstream_tries, prev->upstream.next_upstream_tries, 0);
  ngx_conf_merge_msec_value(conf->upstream.connect_timeout, prev->upstream.connect_timeout, 60000);
  ngx_conf_merge_msec_value(conf->upstream.send_timeout, prev->upstream.send_timeout, 60000);
  ngx_conf_merge_msec_value(conf->upstream.read_timeout, prev->upstream.read_timeout, 60000);
  ngx_conf_merge_msec_value(conf->upstream.next_upstream_timeout, prev->upstream.next_upstream_timeout, 0);
  ngx_conf_merge_size_value(conf->upstream.buffer_size, prev->upstream.buffer_size, (size_t)ngx_pagesize);
  ngx_conf_merge_bitmask_value(conf->upstream.next_upstream, prev->upstream.next_upstream,
                               (NGX_CONF_BITMASK_SET |NGX_HTTP_UPSTREAM_FT_ERROR |NGX_HTTP_UPSTREAM_FT_TIMEOUT));
  
  ngx_hash_init_t hash;
  hash.max_size = 512;
  hash.bucket_size = ngx_align(64, ngx_cacheline_size);
  hash.name = (char*)"erl_headers_hash";
  if (ngx_http_upstream_hide_headers_hash(cf, &conf->upstream, &prev->upstream, ngx_http_cxxrest_hide_headers, &hash) != NGX_OK)
    return (char*)(NGX_CONF_ERROR);
  
  if (conf->upstream.next_upstream & NGX_HTTP_UPSTREAM_FT_OFF)
    conf->upstream.next_upstream = NGX_CONF_BITMASK_SET | NGX_HTTP_UPSTREAM_FT_OFF;

  if (conf->upstream.upstream == NULL)
    conf->upstream.upstream = prev->upstream.upstream;

  return NGX_CONF_OK;
}

static char *ngx_http_cxxrest_pass_handler(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
  ngx_http_cxxrest_loc_conf_t *locationConf = (ngx_http_cxxrest_loc_conf_t*)conf;
  ngx_str_t *values = (ngx_str_t*)cf->args->elts;
  
  ngx_http_core_loc_conf_t *clcf = (ngx_http_core_loc_conf_t*)ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  clcf->handler = ngx_http_cxxrest_handler;
  
  if (cmd->offset == offsetof(ngx_http_cxxrest_loc_conf_t, destination) && cf->args->nelts == 2)
    locationConf->destination = values[1].data;

  ngx_url_t url;
  ngx_memzero(&url, sizeof(url));  
  url.url = values[1];
  url.no_resolve = 1;
  
  locationConf->upstream.upstream = ngx_http_upstream_add(cf, &url, 0);
  if (locationConf->upstream.upstream == NULL) {
    return (char*)(NGX_CONF_ERROR);
  }
  
  ngx_log_error(NGX_LOG_ERR, cf->log, 0, "ngx_http_cxxrest_pass_handler: got destination: %s\n", locationConf->destination);
  
  return NGX_CONF_OK; 
}


static ngx_int_t ngx_http_cxxrest_handler(ngx_http_request_t *r)
{
  if (ngx_http_upstream_create(r) != NGX_OK) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }  
  
  ngx_http_cxxrest_ctx_t *f = (ngx_http_cxxrest_ctx_t*)ngx_pcalloc(r->pool, sizeof(ngx_http_cxxrest_ctx_t));
  if (f == NULL) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  ngx_http_set_ctx(r, f, ngx_http_cxxrest_module);  
  
  ngx_http_cxxrest_loc_conf_t *plcf = (ngx_http_cxxrest_loc_conf_t*)ngx_http_get_module_loc_conf(r, ngx_http_cxxrest_module);  
  
  ngx_http_upstream_t *upstream = r->upstream;

  upstream->output.tag = (ngx_buf_tag_t) &ngx_http_cxxrest_module;
  upstream->create_request = ngx_http_cxxrest_create_request;
  upstream->reinit_request = ngx_http_cxxrest_reinit_request;
  upstream->process_header = ngx_http_cxxrest_process_header;
  upstream->abort_request = ngx_http_cxxrest_abort_request;
  upstream->finalize_request = ngx_http_cxxrest_finalize_request;
  upstream->conf = &plcf->upstream;
  
  upstream->input_filter_init = ngx_http_cxxrest_non_buffered_filter_init;
  upstream->input_filter = ngx_http_cxxrest_non_buffered_filter;
  upstream->input_filter_ctx = r;  

  ngx_int_t rc = ngx_http_read_client_request_body(r, ngx_http_upstream_init);

  if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
    return rc;
  }  
  
  return NGX_DONE;
}

static ngx_int_t ngx_http_cxxrest_non_buffered_filter_init(void *data)
{
    return NGX_OK;
}

static ngx_int_t ngx_http_cxxrest_non_buffered_filter(void *data, ssize_t bytes)
{
  ngx_http_request_t  *r = (ngx_http_request_t*)data;
  ngx_http_cxxrest_ctx_t *requestCtx =
    (ngx_http_cxxrest_ctx_t*)ngx_http_get_module_ctx(r, ngx_http_cxxrest_module);    

  ngx_buf_t            *b;
  ngx_chain_t          *cl, **ll;
  ngx_http_upstream_t  *u;

  u = r->upstream;

  for (cl = u->out_bufs, ll = &u->out_bufs; cl; cl = cl->next) {
    ll = &cl->next;
  }

  cl = ngx_chain_get_free_buf(r->pool, &u->free_bufs);
  if (cl == NULL) {
    return NGX_ERROR;
  }

  *ll = cl;

  cl->buf->flush = 1;
  cl->buf->memory = 1;

  b = &u->buffer;

  cl->buf->pos = b->last;
  b->last += bytes;
  cl->buf->last = b->last;
  cl->buf->tag = u->output.tag;

  requestCtx->bodySize -= std::min(requestCtx->bodySize, bytes);
  u->length = requestCtx->bodySize;
  return NGX_OK;
}


static ngx_int_t ngx_http_cxxrest_create_request(ngx_http_request_t *r)
{
  ngx_uint_t i;

  // Формирование запроса
  // {Method::binary(), Url::binary(), [{HeaderName::binary(), HeaderValue::binary()}], Body::binary()}
  xmstream stream;
  stream.write<uint64_t>(0);
  
  // Method::binary()
  {
    const char *name = methodName(r->method);
    size_t length = strlen(name);
    stream.write<uint32_t>(length);
    stream.write(name, length);
  }
  
  // Url::binary()
  stream.write<uint32_t>(r->unparsed_uri.len);
  stream.write(r->unparsed_uri.data, r->unparsed_uri.len);
  
  // [{HeaderName::binary(), HeaderValue::binary()}]
  size_t headersCounterOff = stream.offsetOf();
  stream.write<uint32_t>(0);
  unsigned headersCount = 0;
  ngx_list_t headersList = r->headers_in.headers;
  ngx_list_part_t *part = &headersList.part;
  while (part) {
    ngx_table_elt_t *headers = (ngx_table_elt_t*)part->elts;
    for (i = 0; i < part->nelts; i++) {
      headersCount++;
      stream.write<uint32_t>(headers[i].key.len);
      stream.write(headers[i].key.data, headers[i].key.len);
      stream.write<uint32_t>(headers[i].value.len);
      stream.write(headers[i].value.data, headers[i].value.len);
    }
    part = part->next;
  }

  *(uint32_t*)(stream.data<uint8_t>() + headersCounterOff) = headersCount;
  
  // Body::binary()
  // TODO: передать тело
  stream.write<uint32_t>(4);
  stream.write("body", 4);

  stream.seekSet(0);
  stream.writeNetworkByteOrder<uint64_t>(stream.sizeOf()-8);  
  
  ngx_buf_t *msgBuf = ngx_create_temp_buf(r->pool, stream.sizeOf());
  if (msgBuf == NULL)
    return NGX_ERROR;

  ngx_chain_t *msgChain = ngx_alloc_chain_link(r->pool);
  if (msgChain == NULL)
    return NGX_ERROR;

  msgChain->buf = msgBuf;
  msgChain->next = NULL;
  r->upstream->request_bufs = msgChain;
  
  memcpy(msgBuf->pos, stream.data(), stream.sizeOf());
  msgBuf->last = msgBuf->pos + stream.sizeOf();
  return NGX_OK;
}

static ngx_int_t ngx_http_cxxrest_reinit_request(ngx_http_request_t *r)
{  
  return NGX_OK;
}

static ngx_int_t ngx_http_cxxrest_process_header(ngx_http_request_t *r)
{
  ngx_http_cxxrest_ctx_t *requestCtx =
    (ngx_http_cxxrest_ctx_t*)ngx_http_get_module_ctx(r, ngx_http_cxxrest_module);
  ngx_http_upstream_t *upstream = r->upstream;

  xmstream stream(upstream->buffer.pos, upstream->buffer.last - upstream->buffer.pos);

  ngx_uint_t responseCode = 500;
  
  // Код ответа
  responseCode = stream.read<uint32_t>();
  
  // Заголовки
  size_t headersNum = stream.read<uint32_t>();
  for (size_t i = 0; i < headersNum; i++) {
    size_t keySize = stream.read<uint32_t>();
    void *key = stream.jumpOver<uint8_t>(keySize);
    size_t valueSize = stream.read<uint32_t>();
    void *value = stream.jumpOver<uint8_t>(valueSize);
    
    // TODO:
    // Проверить необходимость создания строк, оканчивающихся нулем, заполнения lowercase key и hash
    ngx_table_elt_t *header = (ngx_table_elt_t*)ngx_list_push(&upstream->headers_in.headers);
    header->key.len = keySize;
    header->key.data = (u_char*)key;
    header->value.len = valueSize;
    header->value.data = (u_char*)value;
    
    header->lowcase_key = (u_char*)ngx_pnalloc(r->pool, header->key.len);
    ngx_strlow(header->lowcase_key, header->key.data, header->key.len);
  }
    
  requestCtx->bodySize = stream.read<uint32_t>();
  
  // TODO send status line from backend
  ngx_str_set(&upstream->headers_in.status_line, "200 OK");
  upstream->headers_in.status_n = responseCode;
  upstream->buffer.pos = stream.ptr<u_char>(); 
  upstream->buffer.last = upstream->buffer.pos + stream.remaining();
  upstream->length = 0;
  upstream->keepalive = 1;
  
  return !stream.eof() ? NGX_OK : NGX_ERROR;
}

static void ngx_http_cxxrest_abort_request(ngx_http_request_t *r)
{
}

static void ngx_http_cxxrest_finalize_request(ngx_http_request_t *r, ngx_int_t rc)
{
}

static inline const char *methodName(ngx_uint_t method)
{
  switch (method) {
    case NGX_HTTP_GET : return "GET";
    case NGX_HTTP_HEAD : return "HEAD";
    case NGX_HTTP_POST : return "POST";
    case NGX_HTTP_PUT : return "PUT";
    case NGX_HTTP_DELETE : return "DELETE";
    case NGX_HTTP_MKCOL : return "MKCOL";
    case NGX_HTTP_COPY : return "COPY";
    case NGX_HTTP_MOVE : return "MOVE";
    case NGX_HTTP_OPTIONS : return "OPTIONS";
    case NGX_HTTP_PROPFIND : return "PROPFIND";
    case NGX_HTTP_PROPPATCH : return "PROPPATCH";
    case NGX_HTTP_LOCK : return "LOCK";
    case NGX_HTTP_UNLOCK : return "UNLOCK";    
    case NGX_HTTP_PATCH : return "PATCH";   
    case NGX_HTTP_TRACE : return "TRACE";       
    default : return "UNKNOWN";
  }
}
