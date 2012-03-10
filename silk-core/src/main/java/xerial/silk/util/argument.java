/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xerial.silk.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Command-line argument with no option prefix such as "-" or "--"
 *
 * @author leo
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD })
public @interface argument {

    /**
     * Name of the argument. If nothing is given, field name is used;
     */
    String name() default "";

    /**
     * This argument is required or not. (default = false)
     */
    boolean required() default false;

    /**
     * Argument index (0-origin) among the arguments without option prefix, "-"
     * or "--". The default is 0.
     */
    int index() default 0;

    /**
     * Description of this argument
     */
    String description() default "";
}
