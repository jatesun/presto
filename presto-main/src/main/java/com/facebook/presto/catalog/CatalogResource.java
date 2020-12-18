/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sunjiantao
 * @date 2020-12-14
 */
@Path("/v1/catalog")
public class CatalogResource
{
//
//    @POST
//    @Consumes(MediaType.APPLICATION_JSON)
//    @Produces(MediaType.APPLICATION_JSON)
//    public Response createCatalog(CatalogInfo catalogInfo) {
//        CatalogDao dao = new CatalogDao();
//        try {
//            dao.save(catalogInfo);
//        } catch (Exception e) {
//            return Response.status(Response.Status.EXPECTATION_FAILED).build();
//        }
//        //todo 插入数据到数据库中
//        return Response.status(Response.Status.OK).build();
//    }
//
//    @DELETE
//    @Consumes(MediaType.APPLICATION_JSON)
//    @Produces(MediaType.APPLICATION_JSON)
//    public Response deleteCatalog(CatalogInfo catalogInfo) {
//        //todo 删除数据
//        CatalogDao dao = new CatalogDao();
//        try {
//            dao.delete(catalogInfo.getId());
//        } catch (Exception e) {
//            return Response.status(Response.Status.EXPECTATION_FAILED).build();
//        }
//        return Response.status(Response.Status.OK).build();
//    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCatalog()
    {
        //todo 删除数据
        try {
            List<CatalogTest> result1 = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                CatalogTest test = new CatalogTest();
                test.setCatalogName("te");
                test.setCreator("3");
                result1.add(test);
            }
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(result1);
            return Response.ok(json, MediaType.APPLICATION_JSON).build();
//            return result1;
//            return Response.ok(Response.Status.EXPECTATION_FAILED).build();
        }
        catch (Exception e) {
            throw new RuntimeException("系统内部错误");
        }
    }

//    @GET
//    @Consumes(MediaType.APPLICATION_JSON)
//    @Produces(MediaType.APPLICATION_JSON)
//    public Response getCatalog() {
//        try {
//            return Response.ok(Response.Status.EXPECTATION_FAILED).build();
//        } catch (Exception e) {
//            throw new RuntimeException("系统内部错误");
//        }
//    }
}
