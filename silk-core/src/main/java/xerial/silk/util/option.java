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
 * Annotation for specifying command-line options.
 *
 * @author leo
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER })
public @interface option {

    /**
     * symbol of the option. If this symbol is "h", it handles option "-h".
     * The symbol must be a single character.
     *
     */
    String symbol() default "";

    /**
     * Longer name of the option. If this long name is "help", it handles option
     * "--help"
     *
     */
    String longName() default "";

    /**
     * Variable name used to describe option argument (e.g. --file=VALUE). The
     * default value is capitalized name().
     */
    String varName() default "value";

    /**
     * Description of the option, used to generate a help message of this
     * command-line options.
     */
    String description() default "";

}


